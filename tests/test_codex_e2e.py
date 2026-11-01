from __future__ import annotations
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from codex_support import ROOT


@unittest.skipUnless(os.environ.get("AIVENGERS_WIRE_CODEX_E2E")=="1", "opt in to installed-Codex local-stand-in acceptance")
class InstalledCodexTests(unittest.TestCase):
    def test_two_sessions_resume_and_plain_client_without_egress(self):
        self.assertTrue(shutil.which('codex'))
        self.assertTrue(shutil.which('unshare'))
        with tempfile.TemporaryDirectory(prefix='w2-e2e-',dir='/tmp') as d:
            output=Path(d)/'result.json'
            r=subprocess.run(['unshare','-Urn','sh','-c','ip link set lo up && exec "$@"','w2',sys.executable,str(ROOT/'tests'/'codex_e2e.py'),'--output',str(output)],
                             cwd=ROOT,capture_output=True,text=True,timeout=150)
            self.assertEqual(r.returncode,0,r.stdout+'\n'+r.stderr)
            self.assertTrue(output.exists())
            destination=os.environ.get('AIVENGERS_WIRE_CODEX_EVIDENCE')
            if destination:
                Path(destination).write_bytes(output.read_bytes())
