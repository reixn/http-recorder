import http_recorder

from typing import Optional
from mitmproxy import ctx, http, command


class HttpRecorder:

    def __init__(self):
        self.recorder: http_recorder.Recorder = None

    def load(self, loader):
        loader.add_option(
            name="record_dest",
            typespec=str,
            default="record.tar.xz",
            help="path to http record file",
        )
        loader.add_option(
            name="save_duration",
            typespec=int,
            default=10,
            help="auto save duration",
        )
        loader.add_option(
            name="last_log",
            typespec=Optional[str],
            default=None,
            help="path to last log file",
        )

    def configure(self, update):
        if "record_dest" in update or "save_duration" in update or "last_log" in update:
            self.recorder = http_recorder.Recorder(
                ctx.options.record_dest,
                ctx.options.save_duration,
                ctx.options.last_log,
            )

    def done(self):
        self.recorder.finish()

    def response(self, flow: http.HTTPFlow):
        if flow.error == None:
            self.recorder.add_flow(flow)

    @command.command("http-recorder.save")
    def save(self):
        self.recorder.save_tar()


addons = [HttpRecorder()]