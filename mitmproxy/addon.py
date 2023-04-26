import http_recorder

from typing import Optional
from mitmproxy import ctx, http, command

ignore_hosts = {
    "www.googletagmanager.com", "fonts.googleapis.com", "fonts.gstatic.com"
}


def should_add_flow(flow: http.HTTPFlow) -> bool:
    if flow.request.host in ignore_hosts:
        return False
    return True


class HttpRecorder:

    def __init__(self):
        self.recorder: http_recorder.Recorder = None

    def load(self, loader):
        loader.add_option(
            name="record_dest",
            typespec=str,
            default=".",
            help="path to http record file",
        )

    def configure(self, update):
        if "record_dest" in update:
            self.recorder = http_recorder.Recorder(ctx.options.record_dest)

    def done(self):
        self.recorder.finish()

    def response(self, flow: http.HTTPFlow):
        if flow.error == None and should_add_flow(flow):
            self.recorder.add_flow(flow)


addons = [HttpRecorder()]