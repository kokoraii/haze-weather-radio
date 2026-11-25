# Automations

Automations live in `bundle/managed/configs/automations.xml` and are edited from the web panel's Automation Editor. The daemon's scheduler fires them per feed, and the playlist renders the audio.

## Languages

Each automation has one or more `<lang>` blocks:

```xml
<content attention_tone="WXR" reader_id="000">
  <lang code="en"><text>This is a test of {networkName}.</text></lang>
  <lang code="fr-CA" reader_id="02"><text>Ceci est un test de {networkName}.</text></lang>
</content>
```

- Only languages enabled on at least one feed can be added. The editor offers just those, and the server rejects any other on save. A bare code such as `en` is accepted when a feed carries a regional form (`en-US`, `en-CA`).
- On each feed, the blocks for the feed's enabled languages air back to back, in the feed's language order, with one second between them.
- The first block is the default. It airs on a feed that carries none of the automation's languages.
- Each block is spoken in the feed's own language code (an `en` block airs as `en-US` on an `en-US` feed).
- `reader_id` on a block picks its voice. The content-level `reader_id` applies to the first block only. A block without one uses the first male reader for its language in `readers.xml`.

## Variables

Write variables in braces. They are resolved per feed when the automation fires. Unknown names are left as written.

| Variable | Value |
|---|---|
| `{feed-id}` | Feed ID, such as `cwxr-sk01` |
| `{eventType}` | Event name for the SAME code that fired, such as Required Weekly Test (week overrides such as RMT apply) |
| `{transmitterCall}`, `{transmitterCall[n]}` | Transmitter callsign. Index 0 (the default) is the primary transmitter, then the others in feed order |
| `{primaryCoverage}` | Name of the feed's first coverage region, or the primary transmitter's site |
| `{networkName}` | Primary transmitter's network name, or `operator.on_air_name` |
| `{operatorName}` | `operator.operator_name` in `config.yaml` |
| `{phoneNumber}` | `operator.phone_number` in `config.yaml` |
| `{autoHour}`, `{autoMin}`, `{autoAmPm}` | Scheduled time in the feed's local time, 12-hour clock (`12`, `00`, `PM`) |
| `{autoDay}` | Scheduled weekday, such as Wednesday |
| `{timeZone}` | Feed's zone, such as Eastern Daylight Time. Zones without daylight saving read as, for example, Central Time |

Spoken forms (`pronunciation`) are used where the config provides them.
