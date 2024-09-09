from unittest import TestCase
import tempfile
import subprocess
import os.path
import sys
import logging

LOGGER = logging.getLogger(__name__)


class CliTests(TestCase):
    def _execute_sync(self, args, expect_success=True):
        interpreter_path = os.path.abspath(sys.executable)

        args = [
            interpreter_path,
            '-m',
            'sync.cli',
        ] + args

        print('RUNNING: %s' % args)

        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.wait()

        stdout = proc.stdout.read().decode('utf-8')
        stderr = proc.stderr.read().decode('utf-8')

        print('STDOUT:\n%s' % stdout)
        print('STDERR:\n%s' % stderr)

        if expect_success:
            self.assertEqual(0, proc.returncode)

        return proc.returncode

    def test_basic_fs_to_fs_sync(self):
        source_dir = tempfile.mkdtemp()
        target_dir = tempfile.mkdtemp()

        with open(os.path.join(source_dir, 'foo'), 'w') as f:
            f.write('foo')

        with open(os.path.join(source_dir, 'bar'), 'w') as f:
            f.write('bar')

        self._execute_sync([
            '--source',
            'FS',
            'root=%s' % source_dir,
            '--destination',
            'FS',
            'root=%s' % target_dir,
            '--log-level',
            # TODO: for some reason on Windows if this is changed to DEBUG
            #  test never finishes...
            'INFO',
        ])

        self.assertTrue(os.path.exists(os.path.join(target_dir, 'foo')))
        self.assertTrue(os.path.exists(os.path.join(target_dir, 'bar')))

        with open(os.path.join(target_dir, 'foo'), 'r') as f:
            self.assertEqual('foo', f.read())

        with open(os.path.join(target_dir, 'bar'), 'r') as f:
            self.assertEqual('bar', f.read())
