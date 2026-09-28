package ivr

import "testing"

func BenchmarkStaticPromptAudio(b *testing.B) {
	service := staticPromptTestService(b, "default__one_moment", "One moment.")
	values := service.promptValues(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, ok := service.staticPromptAudio("", "one_moment", values); !ok {
			b.Fatal("static prompt unavailable")
		}
	}
}
