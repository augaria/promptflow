import json
import os
import threading
from typing import Any, Dict, List, Iterator, Union

from opentelemetry import trace
from opentelemetry.sdk.trace import Tracer, TracerProvider, Span, StatusCode
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult
)

from promptflow._utils.dataclass_serializer import serialize


# Only for demo purpose, dumping spans to a jsonl file.
# Console/HTTP/AppInsights exporters are already available.
# We might build a new Azure Blob exporter leveraging MDC.
class FileSpanExporter(SpanExporter):
    def __init__(self, file_name: str):
        self.file_name = file_name
        directory = os.path.dirname(file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def export(self, spans):
        with open(self.file_name, "a") as file:
            for span in spans:
                file.write(span.to_json(indent=None) + "\n")
        return SpanExportResult.SUCCESS


# Only for demo purpose. This helper class could be part of the telemetry SDK
class OpenTelemetryTracer:
    _instance: "OpenTelemetryTracer" = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    tracer_provider = TracerProvider()
                    trace.set_tracer_provider(tracer_provider)
                    cls._instance._tracer: Tracer = trace.get_tracer(__name__)
                    cls._instance._span_processors: List[BatchSpanProcessor] = []

        return cls._instance

    def __new__(cls):
        return cls.get_instance()

    @classmethod
    def add_console_span_exporter(cls):
        cls.add_span_exporter(ConsoleSpanExporter())

    @classmethod
    def add_file_span_exporter(cls, file_name: str):
        cls.add_span_exporter(FileSpanExporter(file_name))

    @classmethod
    def add_span_exporter(cls, exporter: SpanExporter):
        span_processor = BatchSpanProcessor(exporter)
        cls.get_instance()._span_processors.append(span_processor)
        trace.get_tracer_provider().add_span_processor(span_processor)

    @classmethod
    def start_as_current_span(cls, name: str, attributes: Dict = None) -> Iterator[Span]:
        return cls.get_instance()._tracer.start_as_current_span(
            name=name,
            attributes=attributes
        )

    @classmethod
    def get_current_span(cls) -> Span:
        return trace.get_current_span()

    @classmethod
    def mark_succeeded(cls):
        cls.get_instance().get_current_span().set_status(StatusCode.OK)

    @classmethod
    def mark_failed(cls):
        cls.get_instance().get_current_span().set_status(StatusCode.ERROR)

    @classmethod
    def set_attribute(cls, key: str, value: Any):
        attribute_value = cls.get_instance()._generate_attribute_value(value)
        cls.get_instance().get_current_span().set_attribute(key, attribute_value)

    @classmethod
    def close(cls):
        for span_processor in cls.get_instance()._span_processors:
            span_processor.shutdown()

    # otel's attribue value must be one of the following types: str, int, float, bool, list
    def _generate_attribute_value(self, obj: object) -> Union[str, int, float, bool, list]:
        value_type = type(obj)
        if value_type in (str, int, float, bool):
            return obj

        if value_type in (list, tuple):
            return [self._generate_attribute_value(item) for item in obj]

        try:
            obj = serialize(obj)
            return json.dumps(obj)
        except Exception:
            return str(obj)

    def __del__(self):
        self.close()
