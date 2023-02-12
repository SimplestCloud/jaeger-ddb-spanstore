package spanstore

import (
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type Plugin struct {
	reader *DdbReader
	writer *DdbWriter
}

var _ shared.StreamingSpanWriterPlugin = &Plugin{}
var _ shared.ArchiveStoragePlugin = &Plugin{}
var _ shared.StoragePlugin = &Plugin{}

func NewPlugin(reader *DdbReader, writer *DdbWriter) *Plugin {
	return &Plugin{
		reader: reader,
		writer: writer,
	}
}

func (p *Plugin) StreamingSpanWriter() spanstore.Writer {
	return p.writer
}

func (p *Plugin) ArchiveSpanReader() spanstore.Reader {
	return p.reader
}

func (p *Plugin) ArchiveSpanWriter() spanstore.Writer {
	return p.writer
}

func (p *Plugin) SpanReader() spanstore.Reader {
	return p.reader
}

func (p *Plugin) SpanWriter() spanstore.Writer {
	return p.writer
}

func (p *Plugin) DependencyReader() dependencystore.Reader {
	return p.reader
}
