package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// laneQueueSize bounds deliveries waiting on one webhook URL. A network
	// fan-out puts one delivery per feed sharing the URL in the same second.
	laneQueueSize = 64
	// maxTranscodes bounds concurrent ffmpeg processes across all lanes.
	maxTranscodes = 4
	// maxRateLimitRetries is how many 429 responses one post retries.
	maxRateLimitRetries = 3
	// maxRateLimitWait caps one rate-limit pause so a bad header cannot park
	// a lane for long.
	maxRateLimitWait = 30 * time.Second
)

// dispatcher posts alerts to Discord. Each webhook URL gets a lane: a FIFO
// queue drained by one worker, so posts to a URL keep event order and follow
// Discord's per-webhook rate limit, while a slow or rate-limited URL never
// holds up another. Audio transcodes start at enqueue and run in parallel
// (bounded), so ffmpeg time overlaps with earlier posts.
type dispatcher struct {
	ctx        context.Context
	client     *http.Client
	transcodes chan struct{}
	mu         sync.Mutex
	lanes      map[string]*lane
}

// delivery is one alert post. audio, when set, yields the transcoded
// attachment (empty when transcoding failed).
type delivery struct {
	feedID string
	label  string
	url    string
	body   discordPayload
	audio  <-chan attachment
}

type lane struct {
	jobs chan delivery
	// readyAt is when Discord allows the next post. Only the lane's worker
	// touches it.
	readyAt time.Time
}

type postResult struct {
	Status int
	Detail string
	Header http.Header
}

// newDispatcher's lane workers run until ctx ends.
func newDispatcher(ctx context.Context, client *http.Client) *dispatcher {
	return &dispatcher{
		ctx:        ctx,
		client:     client,
		transcodes: make(chan struct{}, maxTranscodes),
		lanes:      map[string]*lane{},
	}
}

// enqueue queues job on its URL's lane without blocking. transcode, when not
// nil, starts right away and feeds job.audio. It reports false when the lane
// is full.
func (d *dispatcher) enqueue(job delivery, transcode func() attachment) bool {
	var audio chan attachment
	if transcode != nil {
		audio = make(chan attachment, 1)
		job.audio = audio
	}
	select {
	case d.lane(job.url).jobs <- job:
	default:
		return false
	}
	if audio != nil {
		go func() {
			select {
			case d.transcodes <- struct{}{}:
			case <-d.ctx.Done():
				audio <- attachment{}
				return
			}
			defer func() { <-d.transcodes }()
			audio <- transcode()
		}()
	}
	return true
}

func (d *dispatcher) lane(url string) *lane {
	d.mu.Lock()
	defer d.mu.Unlock()
	l := d.lanes[url]
	if l == nil {
		l = &lane{jobs: make(chan delivery, laneQueueSize)}
		d.lanes[url] = l
		go d.work(l)
	}
	return l
}

func (d *dispatcher) work(l *lane) {
	for {
		select {
		case <-d.ctx.Done():
			return
		case job := <-l.jobs:
			if err := d.deliver(l, job); err != nil {
				log.Printf("[%s] Discord webhook dispatch failed: %v", job.feedID, err)
			}
		}
	}
}

func (d *dispatcher) deliver(l *lane, job delivery) error {
	var audio attachment
	if job.audio != nil {
		audio = <-job.audio
	}
	result, err := d.send(l, job.url, job.body, audio)
	if err != nil {
		return err
	}
	if len(audio.Data) > 0 && (result.Status == http.StatusBadRequest || result.Status == http.StatusRequestEntityTooLarge) {
		log.Printf("[%s] Discord webhook rejected alert attachment, retrying without audio", job.feedID)
		if result, err = d.send(l, job.url, job.body, attachment{}); err != nil {
			return err
		}
	}
	if result.Status < 200 || result.Status >= 300 {
		if result.Detail != "" {
			return fmt.Errorf("HTTP %d: %s", result.Status, clipText(result.Detail, 240))
		}
		return fmt.Errorf("HTTP %d", result.Status)
	}
	log.Printf("[%s] Discord webhook dispatched: %s", job.feedID, job.label)
	return nil
}

// send makes one post, waiting out the lane's rate limit and retrying 429s.
func (d *dispatcher) send(l *lane, url string, body discordPayload, file attachment) (postResult, error) {
	for attempt := 0; ; attempt++ {
		if wait := time.Until(l.readyAt); wait > 0 {
			sleepOrDone(d.ctx, wait)
			if err := d.ctx.Err(); err != nil {
				return postResult{}, err
			}
		}
		result, err := d.post(url, body, file)
		if err != nil {
			return result, err
		}
		if wait := rateLimitWait(result); wait > 0 {
			l.readyAt = time.Now().Add(wait)
		}
		if result.Status != http.StatusTooManyRequests || attempt >= maxRateLimitRetries {
			return result, nil
		}
	}
}

// rateLimitWait reads how long Discord wants before the next post: the 429
// retry delay, or the bucket reset when this request used the bucket up.
// Retry-After (seconds) is preferred: on unversioned webhook URLs the body's
// retry_after may be in milliseconds.
func rateLimitWait(result postResult) time.Duration {
	var wait time.Duration
	if result.Status == http.StatusTooManyRequests {
		if header := result.Header.Get("Retry-After"); header != "" {
			wait = parseSeconds(header)
		} else {
			var body struct {
				RetryAfter float64 `json:"retry_after"`
			}
			if json.Unmarshal([]byte(result.Detail), &body) == nil {
				wait = secondsDuration(body.RetryAfter)
			}
		}
		wait = max(wait, time.Second)
	} else if result.Header.Get("X-RateLimit-Remaining") == "0" {
		wait = parseSeconds(result.Header.Get("X-RateLimit-Reset-After"))
	}
	return min(wait, maxRateLimitWait)
}

func parseSeconds(raw string) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return secondsDuration(seconds)
}

func secondsDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// post sends the payload as JSON, or as multipart when there is an attachment.
func (d *dispatcher) post(url string, payload discordPayload, file attachment) (postResult, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return postResult{}, err
	}
	body := bytes.NewBuffer(payloadBytes)
	contentType := "application/json"
	if len(file.Data) > 0 {
		body = &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		if err := writer.WriteField("payload_json", string(payloadBytes)); err != nil {
			return postResult{}, err
		}
		part, err := writer.CreateFormFile("files[0]", file.Name)
		if err != nil {
			return postResult{}, err
		}
		if _, err := part.Write(file.Data); err != nil {
			return postResult{}, err
		}
		if err := writer.Close(); err != nil {
			return postResult{}, err
		}
		contentType = writer.FormDataContentType()
	}
	req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, url, body)
	if err != nil {
		return postResult{}, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := d.client.Do(req)
	if err != nil {
		return postResult{}, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
	}()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return postResult{Status: resp.StatusCode, Detail: strings.TrimSpace(string(detail)), Header: resp.Header}, nil
}
