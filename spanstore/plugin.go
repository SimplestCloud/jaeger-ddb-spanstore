package spanstore

import (
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type Plugin struct {
}

var _ shared.StreamingSpanWriterPlugin = &Plugin{}
var _ shared.ArchiveStoragePlugin = &Plugin{}
var _ shared.StoragePlugin = &Plugin{}

func (p Plugin) StreamingSpanWriter() spanstore.Writer {
	panic("implement me")
}

func (p Plugin) ArchiveSpanReader() spanstore.Reader {
	//TODO implement me
	panic("implement me")
}

func (p Plugin) ArchiveSpanWriter() spanstore.Writer {
	//TODO implement me
	panic("implement me")
}

func (p Plugin) SpanReader() spanstore.Reader {
	//TODO implement me
	panic("implement me")
}

func (p Plugin) SpanWriter() spanstore.Writer {
	//TODO implement me
	panic("implement me")
}

func (p Plugin) DependencyReader() dependencystore.Reader {
	//TODO implement me
	panic("implement me")
}
