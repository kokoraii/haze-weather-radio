use std::io::Cursor;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};

use anyhow::{Context, Result};
use tracing::{debug, warn};

const COMMAND_QUEUE_CAPACITY: usize = 8;
const TRAY_ICON_PNG: &[u8] = include_bytes!("../../../bundle/webroot/favicon-96x96.png");
const OPEN_PUBLIC_PANEL_LABEL: &str = "Open Public Panel";
const RESTART_ALL_SERVICES_LABEL: &str = "Restart All Services";
const EXIT_LABEL: &str = "Exit";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TrayCommand {
    OpenPublicPanel,
    RestartAllServices,
    Exit,
}

pub(crate) struct TrayController {
    commands: Receiver<TrayCommand>,
    _platform: platform::PlatformTray,
}

impl TrayController {
    pub(crate) fn start() -> Option<Self> {
        let (sender, commands) = sync_channel(COMMAND_QUEUE_CAPACITY);
        let expected = platform::desktop_tray_expected();
        match platform::start(sender) {
            Ok(platform) => {
                debug!("desktop tray started");
                Some(Self {
                    commands,
                    _platform: platform,
                })
            }
            Err(err) => {
                if expected {
                    warn!("desktop tray unavailable: {err:#}");
                } else {
                    debug!("desktop tray unavailable: {err:#}");
                }
                None
            }
        }
    }

    pub(crate) fn try_recv(&self) -> Option<TrayCommand> {
        self.commands.try_recv().ok()
    }
}

pub(crate) fn open_public_panel(url: &str) -> Result<()> {
    webbrowser::open(url).with_context(|| format!("failed to open public panel {url}"))
}

fn queue_command(sender: &SyncSender<TrayCommand>, command: TrayCommand) {
    match sender.try_send(command) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => warn!("desktop tray command queue is full"),
        Err(TrySendError::Disconnected(_)) => {
            debug!("desktop tray command ignored during shutdown");
        }
    }
}

struct DecodedIcon {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
}

fn decode_tray_icon() -> Result<DecodedIcon> {
    let cursor = Cursor::new(TRAY_ICON_PNG);
    let mut decoder = png::Decoder::new(cursor);
    decoder.set_transformations(png::Transformations::normalize_to_color8());
    let mut reader = decoder
        .read_info()
        .context("failed to read tray icon PNG")?;
    let buffer_size = reader
        .output_buffer_size()
        .context("tray icon PNG is too large to decode")?;
    let mut buffer = vec![0; buffer_size];
    let info = reader
        .next_frame(&mut buffer)
        .context("failed to decode tray icon PNG")?;
    let decoded = &buffer[..info.buffer_size()];
    let pixel_count = (info.width as usize)
        .checked_mul(info.height as usize)
        .context("tray icon dimensions overflow")?;
    let mut rgba = Vec::with_capacity(
        pixel_count
            .checked_mul(4)
            .context("tray icon buffer size overflow")?,
    );

    match info.color_type {
        png::ColorType::Rgba => rgba.extend_from_slice(decoded),
        png::ColorType::Rgb => {
            for pixel in decoded.chunks_exact(3) {
                rgba.extend_from_slice(&[pixel[0], pixel[1], pixel[2], 255]);
            }
        }
        png::ColorType::GrayscaleAlpha => {
            for pixel in decoded.chunks_exact(2) {
                rgba.extend_from_slice(&[pixel[0], pixel[0], pixel[0], pixel[1]]);
            }
        }
        png::ColorType::Grayscale => {
            for value in decoded {
                rgba.extend_from_slice(&[*value, *value, *value, 255]);
            }
        }
        png::ColorType::Indexed => anyhow::bail!("tray icon PNG palette was not expanded"),
    }

    anyhow::ensure!(
        rgba.len() == pixel_count * 4,
        "tray icon PNG decoded to an unexpected buffer size"
    );
    Ok(DecodedIcon {
        width: info.width,
        height: info.height,
        rgba,
    })
}

#[cfg(any(test, target_os = "linux"))]
fn rgba_to_argb(mut rgba: Vec<u8>) -> Vec<u8> {
    for pixel in rgba.chunks_exact_mut(4) {
        pixel.rotate_right(1);
    }
    rgba
}

#[cfg(windows)]
mod platform {
    use std::mem;
    use std::ptr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{sync_channel, SyncSender};
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use anyhow::{Context, Result};
    use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
    use tray_icon::{Icon, MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        DispatchMessageW, PeekMessageW, TranslateMessage, MSG, PM_REMOVE, WM_QUIT,
    };

    use super::{
        decode_tray_icon, queue_command, TrayCommand, EXIT_LABEL, OPEN_PUBLIC_PANEL_LABEL,
        RESTART_ALL_SERVICES_LABEL,
    };

    const OPEN_MENU_ID: &str = "haze.open-public-panel";
    const RESTART_MENU_ID: &str = "haze.restart-all-services";
    const EXIT_MENU_ID: &str = "haze.exit";

    pub(crate) struct PlatformTray {
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    pub(crate) fn desktop_tray_expected() -> bool {
        true
    }

    pub(crate) fn start(commands: SyncSender<TrayCommand>) -> Result<PlatformTray> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let (ready_sender, ready_receiver) = sync_channel::<Result<(), String>>(1);
        let thread = thread::Builder::new()
            .name("haze-tray".to_string())
            .spawn(move || {
                if let Err(err) = run_tray(commands, thread_stop, &ready_sender) {
                    let message = format!("{err:#}");
                    let _ = ready_sender.try_send(Err(message.clone()));
                    tracing::warn!("desktop tray stopped: {message}");
                }
            })
            .context("failed to start desktop tray thread")?;

        match ready_receiver.recv_timeout(Duration::from_secs(3)) {
            Ok(Ok(())) => Ok(PlatformTray {
                stop,
                thread: Some(thread),
            }),
            Ok(Err(message)) => {
                stop.store(true, Ordering::Release);
                let _ = thread.join();
                anyhow::bail!(message)
            }
            Err(err) => {
                stop.store(true, Ordering::Release);
                anyhow::bail!("desktop tray did not initialize: {err}")
            }
        }
    }

    fn run_tray(
        commands: SyncSender<TrayCommand>,
        stop: Arc<AtomicBool>,
        ready: &SyncSender<Result<(), String>>,
    ) -> Result<()> {
        let decoded = decode_tray_icon()?;
        let icon = Icon::from_rgba(decoded.rgba, decoded.width, decoded.height)
            .context("failed to create Windows tray icon")?;
        let menu = Menu::new();
        let open = MenuItem::with_id(OPEN_MENU_ID, OPEN_PUBLIC_PANEL_LABEL, true, None);
        let restart = MenuItem::with_id(RESTART_MENU_ID, RESTART_ALL_SERVICES_LABEL, true, None);
        let exit = MenuItem::with_id(EXIT_MENU_ID, EXIT_LABEL, true, None);
        let separator_one = PredefinedMenuItem::separator();
        let separator_two = PredefinedMenuItem::separator();
        menu.append_items(&[&open, &separator_one, &restart, &separator_two, &exit])
            .context("failed to create Windows tray menu")?;

        let _tray = TrayIconBuilder::new()
            .with_id("haze-weather-radio")
            .with_icon(icon)
            .with_tooltip("Haze Weather Radio")
            .with_menu(Box::new(menu))
            .with_menu_on_left_click(false)
            .build()
            .context("failed to create Windows tray icon")?;
        let _ = ready.send(Ok(()));

        while !stop.load(Ordering::Acquire) {
            if !pump_messages() {
                break;
            }
            while let Ok(event) = MenuEvent::receiver().try_recv() {
                match event.id.as_ref() {
                    OPEN_MENU_ID => queue_command(&commands, TrayCommand::OpenPublicPanel),
                    RESTART_MENU_ID => {
                        queue_command(&commands, TrayCommand::RestartAllServices);
                    }
                    EXIT_MENU_ID => queue_command(&commands, TrayCommand::Exit),
                    _ => {}
                }
            }
            while let Ok(event) = TrayIconEvent::receiver().try_recv() {
                if matches!(
                    event,
                    TrayIconEvent::Click {
                        button: MouseButton::Left,
                        button_state: MouseButtonState::Up,
                        ..
                    }
                ) {
                    queue_command(&commands, TrayCommand::OpenPublicPanel);
                }
            }
            thread::sleep(Duration::from_millis(25));
        }
        Ok(())
    }

    fn pump_messages() -> bool {
        // SAFETY: MSG is zero-initializable, and the Win32 calls receive a valid
        // mutable pointer for the duration of each call on the tray thread.
        unsafe {
            let mut message: MSG = mem::zeroed();
            while PeekMessageW(&mut message, ptr::null_mut(), 0, 0, PM_REMOVE) != 0 {
                if message.message == WM_QUIT {
                    return false;
                }
                TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }
        true
    }

    impl Drop for PlatformTray {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::env;
    use std::sync::mpsc::SyncSender;

    use anyhow::{Context, Result};
    use ksni::blocking::{Handle, TrayMethods};
    use ksni::menu::{MenuItem, StandardItem};

    use super::{
        decode_tray_icon, queue_command, rgba_to_argb, TrayCommand, EXIT_LABEL,
        OPEN_PUBLIC_PANEL_LABEL, RESTART_ALL_SERVICES_LABEL,
    };

    struct LinuxTray {
        commands: SyncSender<TrayCommand>,
        icon: Vec<ksni::Icon>,
    }

    impl LinuxTray {
        fn queue(&self, command: TrayCommand) {
            queue_command(&self.commands, command);
        }
    }

    impl ksni::Tray for LinuxTray {
        fn id(&self) -> String {
            "haze-weather-radio".to_string()
        }

        fn title(&self) -> String {
            "Haze Weather Radio".to_string()
        }

        fn activate(&mut self, _x: i32, _y: i32) {
            self.queue(TrayCommand::OpenPublicPanel);
        }

        fn icon_pixmap(&self) -> Vec<ksni::Icon> {
            self.icon.clone()
        }

        fn tool_tip(&self) -> ksni::ToolTip {
            ksni::ToolTip {
                icon_pixmap: self.icon.clone(),
                title: "Haze Weather Radio".to_string(),
                description: "Haze services and public panel".to_string(),
                ..Default::default()
            }
        }

        fn menu(&self) -> Vec<MenuItem<Self>> {
            vec![
                StandardItem {
                    label: OPEN_PUBLIC_PANEL_LABEL.to_string(),
                    activate: Box::new(|tray: &mut LinuxTray| {
                        tray.queue(TrayCommand::OpenPublicPanel);
                    }),
                    ..Default::default()
                }
                .into(),
                MenuItem::Separator,
                StandardItem {
                    label: RESTART_ALL_SERVICES_LABEL.to_string(),
                    activate: Box::new(|tray: &mut LinuxTray| {
                        tray.queue(TrayCommand::RestartAllServices);
                    }),
                    ..Default::default()
                }
                .into(),
                MenuItem::Separator,
                StandardItem {
                    label: EXIT_LABEL.to_string(),
                    icon_name: "application-exit".to_string(),
                    activate: Box::new(|tray: &mut LinuxTray| tray.queue(TrayCommand::Exit)),
                    ..Default::default()
                }
                .into(),
            ]
        }

        fn watcher_offline(&self, reason: ksni::OfflineReason) -> bool {
            tracing::debug!("Linux tray watcher offline: {reason:?}");
            true
        }
    }

    pub(crate) struct PlatformTray {
        handle: Handle<LinuxTray>,
    }

    pub(crate) fn desktop_tray_expected() -> bool {
        desktop_session_available()
    }

    pub(crate) fn start(commands: SyncSender<TrayCommand>) -> Result<PlatformTray> {
        anyhow::ensure!(
            desktop_session_available(),
            "no graphical Linux desktop session is available"
        );
        let decoded = decode_tray_icon()?;
        let width = i32::try_from(decoded.width).context("tray icon width is too large")?;
        let height = i32::try_from(decoded.height).context("tray icon height is too large")?;
        let tray = LinuxTray {
            commands,
            icon: vec![ksni::Icon {
                width,
                height,
                data: rgba_to_argb(decoded.rgba),
            }],
        };
        let handle = tray
            .spawn()
            .context("failed to register Linux status notifier item")?;
        Ok(PlatformTray { handle })
    }

    fn desktop_session_available() -> bool {
        let graphical =
            env::var_os("DISPLAY").is_some() || env::var_os("WAYLAND_DISPLAY").is_some();
        let session_bus = env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some()
            || env::var_os("XDG_RUNTIME_DIR").is_some();
        graphical && session_bus
    }

    impl Drop for PlatformTray {
        fn drop(&mut self) {
            if !self.handle.is_closed() {
                self.handle.shutdown().wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_tray_icon_decodes_to_rgba() {
        let icon = decode_tray_icon().expect("bundled tray icon should decode");

        assert_eq!((icon.width, icon.height), (96, 96));
        assert_eq!(icon.rgba.len(), 96 * 96 * 4);
    }

    #[test]
    fn linux_icon_conversion_moves_alpha_first() {
        assert_eq!(
            rgba_to_argb(vec![10, 20, 30, 40, 50, 60, 70, 80]),
            vec![40, 10, 20, 30, 80, 50, 60, 70]
        );
    }

    #[test]
    fn tray_menu_contract_uses_the_requested_labels() {
        assert_eq!(OPEN_PUBLIC_PANEL_LABEL, "Open Public Panel");
        assert_eq!(RESTART_ALL_SERVICES_LABEL, "Restart All Services");
        assert_eq!(EXIT_LABEL, "Exit");
    }

    #[test]
    fn tray_command_queue_is_bounded_and_non_blocking() {
        let (sender, receiver) = sync_channel(1);
        queue_command(&sender, TrayCommand::OpenPublicPanel);
        queue_command(&sender, TrayCommand::RestartAllServices);

        assert_eq!(receiver.try_recv(), Ok(TrayCommand::OpenPublicPanel));
        assert!(receiver.try_recv().is_err());
    }
}
