from dbt.task.base import ConfiguredTask
from dbt.logger import print_timestamped_line
from dbt.perf_utils import get_full_manifest
from dbt.adapters.factory import get_adapter
from dbt.parser.manifest import load_manifest
from dbt import flags
from typing import Optional
import os

MANIFEST_FILE_NAME = 'manifest.json'

class ParseTask(ConfiguredTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.manifest: Optional[Manifest] = None
        self.graph: Optional[Graph] = None

    def write_manifest(self):
        path = os.path.join(self.config.target_path, MANIFEST_FILE_NAME)
        self.manifest.write(path)

    def get_full_manifest(self):
        adapter = get_adapter(self.config)  # type: ignore
        macro_manifest: Manifest = adapter.load_macro_manifest()
        print_timestamped_line("Macro manifest loaded")
        self.manifest = load_manifest(
            self.config,
            macro_manifest,
            adapter.connections.set_query_header,
        )
        print_timestamped_line("Manifest loaded")

    def compile_manifest(self):
        adapter = get_adapter(self.config)
        compiler = adapter.get_compiler()
        self.graph = compiler.compile(self.manifest)

    def run(self):
        print_timestamped_line('Start parsing.')
        self.get_full_manifest()
        if self.args.compile:
            print_timestamped_line('Compiling.')
            self.compile_manifest()
        if self.args.write_manifest:
            print_timestamped_line('Writing manifest.')
            self.write_manifest()
        print_timestamped_line('Done.')

