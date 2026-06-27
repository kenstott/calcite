// Minimal hugot embedding server: JSON request per line on stdin, JSON response per line on stdout.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/options"
	"github.com/knights-analytics/hugot/pipelines"
)

func main() {
	ctx := context.Background()
	var opts []options.WithOption
	if lib := os.Getenv("ORT_LIB_PATH"); lib != "" {
		opts = append(opts, options.WithOnnxLibraryPath(lib))
	}
	session, err := hugot.NewORTSession(ctx, opts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "session:", err)
		os.Exit(1)
	}
	defer func() { _ = session.Destroy() }()

	cfg := hugot.FeatureExtractionConfig{
		ModelPath: os.Getenv("EMBED_MODEL_PATH"),
		Name:      "emb",
	}
	cfg.Options = append(cfg.Options, pipelines.WithNormalization())
	pipe, err := hugot.NewPipeline(session, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "hugot-embed: serve ready")

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	for in.Scan() {
		var req struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			_ = json.NewEncoder(out).Encode(map[string]string{"error": err.Error()})
			out.Flush()
			continue
		}
		res, err := pipe.RunPipeline(ctx, []string{req.Text})
		if err != nil {
			_ = json.NewEncoder(out).Encode(map[string]string{"error": err.Error()})
			out.Flush()
			continue
		}
		_ = json.NewEncoder(out).Encode(map[string][]float32{"embedding": res.Embeddings[0]})
		out.Flush()
	}
}
