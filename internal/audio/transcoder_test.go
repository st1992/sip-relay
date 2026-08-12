package audio

import (
	"reflect"
	"sync"
	"testing"
)

func TestInputTranscoderChunkContinuity(t *testing.T) {
	// Real RTP delivers PCMU in fixed 160-byte (20ms @ 8kHz) frames.
	tone := sineWave(4000, 8000, 300, 10000)
	mulaw := EncodeMulaw(nil, tone)

	whole := NewInputTranscoder(16000)
	want := whole.Transcode(mulaw)

	chunked := NewInputTranscoder(16000)
	var got []byte
	const frame = 160
	for i := 0; i < len(mulaw); i += frame {
		end := i + frame
		if end > len(mulaw) {
			end = len(mulaw)
		}
		got = append(got, chunked.Transcode(mulaw[i:end])...)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chunked InputTranscoder output diverged from one-shot: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func TestOutputTranscoderHandlesArbitraryMessageChunking(t *testing.T) {
	tone := sineWave(6000, 24000, 300, 10000)
	pcm16 := SamplesToBytesLE(nil, tone)

	whole := NewOutputTranscoder(24000)
	want := whole.Transcode(pcm16)

	chunked := NewOutputTranscoder(24000)
	var got []byte
	sizes := []int{7, 13, 21, 3, 50} // deliberately includes odd sizes
	pos, si := 0, 0
	for pos < len(pcm16) {
		size := sizes[si%len(sizes)]
		si++
		end := pos + size
		if end > len(pcm16) {
			end = len(pcm16)
		}
		got = append(got, chunked.Transcode(pcm16[pos:end])...)
		pos = end
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chunked OutputTranscoder output diverged from one-shot: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func TestOutputTranscoderShortMessageHandledGracefully(t *testing.T) {
	tr := NewOutputTranscoder(24000)
	out := tr.Transcode([]byte{0x01}) // one lone byte: no complete sample yet
	if len(out) != 0 {
		t.Fatalf("Transcode of a single odd byte produced %d bytes, want 0", len(out))
	}
	// The buffered byte should combine correctly with the next message.
	out = tr.Transcode([]byte{0x02, 0x03, 0x04})
	if out == nil {
		t.Fatal("Transcode returned nil, want a non-nil (possibly empty) slice")
	}
}

func TestTranscodersAreIndependentAcrossGoroutines(t *testing.T) {
	toneA := sineWave(2000, 8000, 300, 10000)
	toneB := sineWave(2000, 8000, 440, 8000)
	mulawA := EncodeMulaw(nil, toneA)
	mulawB := EncodeMulaw(nil, toneB)

	wantA := NewInputTranscoder(16000).Transcode(mulawA)
	wantB := NewInputTranscoder(16000).Transcode(mulawB)

	var wg sync.WaitGroup
	var gotA, gotB []byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		gotA = NewInputTranscoder(16000).Transcode(mulawA)
	}()
	go func() {
		defer wg.Done()
		gotB = NewInputTranscoder(16000).Transcode(mulawB)
	}()
	wg.Wait()

	if !reflect.DeepEqual(gotA, wantA) {
		t.Fatal("concurrent InputTranscoder A produced unexpected output")
	}
	if !reflect.DeepEqual(gotB, wantB) {
		t.Fatal("concurrent InputTranscoder B produced unexpected output")
	}
}
