#!/usr/bin/env python3
"""Exercise isolated render/teardown contracts without touching real pods."""
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRATCH = ROOT / 'build/devenv-lifecycle-tests'
SCRATCH.mkdir(parents=True, exist_ok=True)

class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.root = Path(self.temp.name) / 'checkout with spaces @literal@'
        (self.root / 'scripts').mkdir(parents=True)
        (self.root / 'devenv').mkdir()
        (self.root / 'devenv/static.txt').write_text('preserved')
        shutil.copyfile(ROOT / 'devenv/client.yaml', self.root / 'devenv/client.yaml')
        for name in ('devenv.sh', 'render-devenv.py', 'probe-devenv-disk.py',
                     'assert_generated_path.sh'):
            shutil.copyfile(ROOT / 'scripts' / name, self.root / 'scripts' / name)
        shutil.copyfile(ROOT / 'devenv.yaml.in', self.root / 'devenv.yaml.in')
        self.bin = Path(self.temp.name) / 'bin'
        self.bin.mkdir()
        self.log = Path(self.temp.name) / 'podman.log'
        fake = self.bin / 'podman'
        fake.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$FAKE_PODMAN_LOG"
case "$1 $2" in
  'info --format') echo "${FAKE_ROOTLESS:-true}";;
  'pod exists')
    if [ -f "$FAKE_PODMAN_LOG.down" ]; then exit "${FAKE_POST_DOWN_POD_RC:-1}";
    else exit "${FAKE_EXISTS_RC:-1}"; fi;;
  'container exists')
    if [[ "$3" == *"${FAKE_RESIDUAL_CONTAINER:-no-service}" ]]; then exit 0;
    else exit "${FAKE_POST_DOWN_CONTAINER_RC:-1}"; fi;;
  'container inspect')
    if [[ "${*: -1}" == *"${FAKE_STOPPED_SERVICE:-no-service}" ]]; then echo false;
    else echo "${FAKE_RUNNING:-false}"; fi;;
  'pod stop') exit "${FAKE_STOP_RC:-0}";;
  'cp --archive=false') exit "${FAKE_CP_RC:-0}";;
  'kube play') exit "${FAKE_PLAY_RC:-31}";;
  'kube down')
    if [ "${FAKE_DOWN_RC:-0}" = 0 ]; then touch "$FAKE_PODMAN_LOG.down"; fi
    exit "${FAKE_DOWN_RC:-0}";;
esac
''')
        fake.chmod(0o700)
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith('LOCKDC_')}
        self.env.update(PATH=str(self.bin) + ':' + os.environ['PATH'],
                        FAKE_PODMAN_LOG=str(self.log))
        self.workspace = self.root / 'build/devenv'
        self.listeners = []

    def tearDown(self):
        for listener in self.listeners:
            listener.close()
        self.temp.cleanup()

    def run_command(self, action, success=True, **overrides):
        result = subprocess.run(['bash', str(self.root / 'scripts/devenv.sh'), action],
                                cwd=self.bin, env=self.env | overrides,
                                text=True, capture_output=True, timeout=10)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def manifest(self):
        return json.loads((self.workspace / 'devenv.yaml').read_text())

    def test_render_is_deterministic_private_and_checkout_scoped(self):
        self.run_command('render')
        original = (self.workspace / 'devenv.yaml').read_bytes()
        self.run_command('render')
        self.assertEqual(original, (self.workspace / 'devenv.yaml').read_bytes())
        self.assertEqual((self.workspace / 'devenv.yaml').stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.workspace / 'env.sh').stat().st_mode & 0o777, 0o600)
        pod = self.manifest()
        self.assertTrue(pod['metadata']['name'].startswith('liblockdc-'))
        ports = [port for c in pod['spec']['containers'] for port in c.get('ports', [])]
        self.assertEqual(len({p['hostPort'] for p in ports}), 5)
        self.assertTrue(all(p['hostIP'] == '127.0.0.1' and p['hostPort'] > 1023 for p in ports))
        for volume in pod['spec']['volumes']:
            self.assertTrue(Path(volume['hostPath']['path']).is_relative_to(self.workspace))
            self.assertTrue(Path(volume['hostPath']['path']).is_dir())
        moved = self.root.parent / 'second checkout'
        shutil.copytree(self.root, moved, ignore=shutil.ignore_patterns('build'))
        result = subprocess.run(['python3', str(moved / 'scripts/render-devenv.py')],
                                env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        second = json.loads((moved / 'build/devenv/devenv.yaml').read_text())
        self.assertNotEqual(second['metadata']['name'], pod['metadata']['name'])
        self.assertNotEqual(second['spec']['containers'][0]['ports'], ports[0:2])

    def test_port_override_validation_and_existing_manifest_preservation(self):
        self.run_command('render', LOCKDC_DISK_A_PORT='60441')
        self.assertEqual(self.manifest()['spec']['containers'][2]['ports'][0]['hostPort'], 60441)
        before = (self.workspace / 'devenv.yaml').read_bytes()
        before_env = (self.workspace / 'env.sh').read_bytes()
        self.run_command('render')
        self.assertEqual(before, (self.workspace / 'devenv.yaml').read_bytes())
        self.assertEqual(before_env, (self.workspace / 'env.sh').read_bytes())
        for port in ('0', '65536', '-2', 'oops', '123;false'):
            self.run_command('render', success=False, LOCKDC_DISK_A_PORT=port)
            self.assertEqual(before, (self.workspace / 'devenv.yaml').read_bytes())
        self.run_command('render', success=False, LOCKDC_DISK_A_PORT='60442')
        self.run_command('render', success=False, LOCKDC_DISK_A_PORT='60441', LOCKDC_S3_PORT='60441')
        self.assertEqual(before, (self.workspace / 'devenv.yaml').read_bytes())

    def test_generated_environment_exports_consumer_disk_aliases(self):
        self.run_command('render', LOCKDC_DISK_A_PORT='60441')
        result = subprocess.run(['bash', '-c',
            'source "$1"; printf "%s\\n" "$LOCKDC_E2E_DISK_A_ENDPOINT" '
            '"$LOCKDC_E2E_DISK_ENDPOINT" "$LOCKDC_E2E_DISK_A_BUNDLE" '
            '"$LOCKDC_E2E_DISK_BUNDLE"', 'bash', str(self.workspace / 'env.sh')],
            env=self.env, capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.splitlines(), ['https://localhost:60441'] * 2 +
                         [str(self.workspace / 'credentials/disk/client.pem')] * 2)

    def test_invalid_saved_ports_are_rejected_without_rewriting(self):
        self.run_command('render')
        saved = self.manifest()
        saved['spec']['containers'][2]['ports'][0]['hostPort'] = 'invalid'
        path = self.workspace / 'devenv.yaml'
        path.write_text(json.dumps(saved))
        before = path.read_bytes()
        result = self.run_command('render', success=False)
        self.assertIn('cannot read saved devenv ports', result.stderr)
        self.assertEqual(before, path.read_bytes())

    def test_disk_primary_selection_preserves_manifest_and_sets_failover(self):
        self.run_command('render', LOCKDC_DISK_A_PORT='60441', LOCKDC_DISK_B_PORT='60442')
        before = (self.workspace / 'devenv.yaml').read_bytes()
        subprocess.run(['python3', str(self.root / 'scripts/render-devenv.py'),
                        '--disk-primary', 'b'], env=self.env, check=True,
                       capture_output=True, text=True)
        self.assertEqual(before, (self.workspace / 'devenv.yaml').read_bytes())
        result = subprocess.run(['bash', '-c',
            'source "$1"; printf "%s\\n" "$LOCKDC_E2E_DISK_ENDPOINT" '
            '"$LOCKDC_E2E_DISK_A_ENDPOINT" "$LOCKDC_URL" "$LOCKDC_FALLBACK_URL"',
            'bash', str(self.workspace / 'env.sh')], env=self.env,
            capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.splitlines(),
                         ['https://localhost:60442'] * 3 + ['https://localhost:60441'])

    def probe(self, active_port, release_rc='0'):
        self.run_command('render')
        fake = self.workspace / 'state/bootstrap/lockd'
        fake.write_text('''#!/usr/bin/env python3
import json, os, sys
assert not any(key.startswith('LOCKD_') for key in os.environ)
args = sys.argv[1:]
assert args[args.index('--timeout') + 1] == '2s'
assert args[args.index('--namespace') + 1] == 'default'
if args[args.index('--server') + 1] != 'https://localhost:' + os.environ['FAKE_ACTIVE_PORT']:
    sys.exit(1)
if 'acquire' in args:
    assert '--retries=-1' in args and '--block=-1' in args
    print(json.dumps({'lease_id': 'lease', 'txn_id': 'txn', 'fencing_token': 3}))
else:
    assert 'release' in args and '--rollback' in args
    assert args[args.index('--lease') + 1] == 'lease'
    assert args[args.index('--txn-id') + 1] == 'txn'
    assert args[args.index('--fencing-token') + 1] == '3'
    sys.exit(int(os.environ['FAKE_RELEASE_RC']))
''')
        fake.chmod(0o700)
        return subprocess.run(['python3', str(self.root / 'scripts/probe-devenv-disk.py'),
                               '60441', '60442'], text=True, capture_output=True,
                              timeout=10, env=self.env | {
                                  'FAKE_ACTIVE_PORT': active_port,
                                  'FAKE_RELEASE_RC': release_rc,
                                  'LOCKD_CLIENT_TXN_ID': 'must-not-inherit'})

    def test_disk_probe_selects_either_leader_and_rolls_back(self):
        for port, node in [('60441', 'a'), ('60442', 'b')]:
            result = self.probe(port)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, node + '\n')

    def test_disk_probe_rejects_missing_leader_or_failed_rollback(self):
        for port, release_rc in [('60443', '0'), ('60442', '1')]:
            result = self.probe(port, release_rc)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, '')

    def test_manifest_preserves_tls_shared_disk_s3_and_uds_graph(self):
        self.run_command('render')
        spec = self.manifest()['spec']
        self.assertEqual(spec['securityContext'], {'runAsUser': 0, 'runAsGroup': 0})
        self.assertEqual(self.manifest()['metadata']['annotations']['io.podman.annotations.userns'], 'keep-id:uid=0,gid=0')
        containers = {c['name']: c for c in spec['containers']}
        a, b = containers['lockd-disk-a'], containers['lockd-disk-b']
        self.assertEqual(a['volumeMounts'], b['volumeMounts'])
        self.assertNotEqual(a['ports'][0]['containerPort'], b['ports'][0]['containerPort'])
        for c in spec['containers'] + spec['initContainers']:
            self.assertNotIn(':latest', c['image'])
            self.assertTrue(c['image'].startswith(('docker.io/', 'quay.io/')))
        server = next(c for c in spec['initContainers'] if c['name'] == 'disk-server')
        self.assertIn('lockd-disk-a,lockd-disk-b,localhost,127.0.0.1', server['args'][0])
        self.assertIn('if [ -s', server['args'][0])
        self.assertNotIn('--force', server['args'][0])
        self.assertIn('s3://localhost:9000/lockd-client-s3/liblockdc?insecure=1&path-style=1', containers['lockd-s3']['args'])
        self.assertIn('--disable-mtls', containers['lockd-mem']['args'])
        self.assertIn('/run/lockd/lockd.sock', containers['lockd-mem']['args'])

    def test_rootful_and_failed_play_are_rejected_with_diagnostics(self):
        result = self.run_command('up', success=False, FAKE_ROOTLESS='false')
        self.assertIn('rootless', result.stderr)
        self.assertNotIn('kube play', self.log.read_text())
        result = self.run_command('up', success=False)
        self.assertEqual(result.returncode, 31)
        log = self.log.read_text()
        self.assertIn('kube play --network pasta --start=false', log)
        self.assertIn('pod ps --filter name=^liblockdc-', log)
        self.assertIn('pod logs liblockdc-', log)
        self.assertNotIn('--wait', log)
        self.assertTrue((self.workspace / 'devenv.yaml').exists())

    def test_failed_teardown_keeps_manifest_and_reset_keeps_static_files(self):
        self.run_command('render')
        marker = self.workspace / 'state/disk/test-state'
        marker.write_text('written')
        self.run_command('reset', success=False, FAKE_DOWN_RC='32')
        self.assertTrue(marker.exists())
        self.assertTrue((self.workspace / 'devenv.yaml').exists())
        self.run_command('reset')
        self.assertFalse(self.workspace.exists())
        self.assertEqual((self.root / 'devenv/static.txt').read_text(), 'preserved')
        self.assertIn(str(self.workspace / 'devenv.yaml'), self.log.read_text())
        self.run_command('reset')

    def test_existing_pod_retries_bootstrap_after_copy_failure(self):
        result = self.run_command('up', success=False,
                                  FAKE_EXISTS_RC='0', FAKE_CP_RC='34')
        self.assertEqual(result.returncode, 34)
        result = self.run_command('up', success=False,
                                  FAKE_EXISTS_RC='0', FAKE_CP_RC='35')
        self.assertEqual(result.returncode, 35)
        log = self.log.read_text()
        self.assertEqual(log.count('cp --archive=false'), 2)
        self.assertNotIn('kube play', log)

    def test_failed_stop_prevents_teardown_and_deletion(self):
        self.run_command('render')
        self.run_command('reset', success=False, FAKE_EXISTS_RC='0', FAKE_STOP_RC='33')
        self.assertTrue((self.workspace / 'devenv.yaml').exists())
        log = self.log.read_text()
        self.assertIn('pod stop --time 60 liblockdc-', log)
        self.assertNotIn('kube down', log)

    def test_running_requires_all_long_lived_services(self):
        self.run_command('render')
        self.run_command('running', FAKE_RUNNING='true')
        for service in ('minio', 'lockd-disk-a', 'lockd-disk-b', 'lockd-s3', 'lockd-mem'):
            self.run_command('running', success=False, FAKE_RUNNING='true',
                             FAKE_STOPPED_SERVICE=service)
        self.run_command('running', success=False, FAKE_RUNNING='false')

    def prepare_e2e_runner(self, ready=False):
        wrapper = self.root / 'scripts/test-e2e.sh'
        shutil.copyfile(ROOT / 'scripts/test-e2e.sh', wrapper)
        (self.root / 'scripts/devenv.sh').chmod(0o700)
        for name, content in [('run_timed.sh', 'shift\nexec "$@"'),
                              ('build.sh', 'exit 0')]:
            script = self.root / 'scripts' / name
            script.write_text('#!/usr/bin/env bash\n' + content + '\n')
            script.chmod(0o700)
        if ready:
            ports = {}
            for variable in ('LOCKDC_DISK_A_PORT', 'LOCKDC_DISK_B_PORT', 'LOCKDC_S3_PORT',
                             'LOCKDC_MINIO_API_PORT', 'LOCKDC_MINIO_CONSOLE_PORT'):
                listener = socket.socket()
                listener.bind(('127.0.0.1', 0))
                listener.listen(10)
                self.listeners.append(listener)
                ports[variable] = str(listener.getsockname()[1])
            self.run_command('render', **ports)
            for name in ('disk', 's3'):
                (self.workspace / f'credentials/{name}/client.pem').write_text('test bundle')
            listener = socket.socket(socket.AF_UNIX)
            # Linux sockaddr_un is limited to 108 bytes; the fixture checkout
            # deliberately has a long path, so bind relative to its state root.
            previous = Path.cwd()
            try:
                os.chdir(self.workspace / 'state/mem-run')
                listener.bind('lockd.sock')
            finally:
                os.chdir(previous)
            self.listeners.append(listener)
            example = self.root / 'build/e2e/examples/lc_example_acquire_lease_lifecycle'
            example.parent.mkdir(parents=True)
            example.write_text('#!/usr/bin/env bash\nexit 0\n')
            example.chmod(0o700)
            # Keep real render/running/down, while injecting startup outcomes.
            real = self.root / 'scripts/devenv-real.sh'
            (self.root / 'scripts/devenv.sh').rename(real)
            stub = self.root / 'scripts/devenv.sh'
            stub.write_text('''#!/usr/bin/env bash
if [ "$1" = up ]; then
  echo 'e2e up' >> "$FAKE_PODMAN_LOG"
  exit "${FAKE_START_RC:-0}"
fi
exec "$(dirname "$0")/devenv-real.sh" "$@"
''')
            stub.chmod(0o700)
            ctest = self.bin / 'ctest'
            ctest.write_text('''#!/usr/bin/env bash
echo "ctest $*" >> "$FAKE_PODMAN_LOG"
if [ "${FAKE_CTEST_WAIT:-0}" = 1 ]; then
  # Handle startup interrupts. Publish readiness only after exec: Bash can
  # discard a pending INT while entering a timed read or replacing itself.
  # Native flock creates the marker with default signal handling, then
  # replaces itself with sleep, keeping one foreground process throughout.
  trap 'exit 130' INT
  trap 'exit 143' TERM
  touch "$FAKE_PODMAN_LOG.ctest-starting"
  exec flock --no-fork "$FAKE_PODMAN_LOG.ctest" sleep 30
fi
touch "$FAKE_PODMAN_LOG.ctest"
exit "${FAKE_CTEST_RC:-0}"
''')
            ctest.chmod(0o700)
        return wrapper

    def run_e2e(self, mode='all', **overrides):
        return subprocess.run(['bash', str(self.root / 'scripts/test-e2e.sh'), mode],
                              cwd=self.bin, env=self.env | overrides,
                              text=True, capture_output=True, timeout=10)

    def clear_podman_log(self):
        self.log.unlink(missing_ok=True)
        Path(str(self.log) + '.down').unlink(missing_ok=True)

    def test_e2e_failed_start_tears_down_even_preexisting_services_unless_kept(self):
        self.prepare_e2e_runner()
        for running, keep, teardown in [('false', '0', True), ('false', '1', False)]:
            self.clear_podman_log()
            result = self.run_e2e(FAKE_RUNNING=running, FAKE_EXISTS_RC='0',
                                 FAKE_CP_RC='36', LOCKDC_E2E_KEEP_DEVSERVICES=keep)
            self.assertEqual(result.returncode, 36, result.stderr)
            self.assertEqual('kube down' in self.log.read_text(), teardown)

    def test_e2e_full_run_owns_started_and_preexisting_stacks(self):
        self.prepare_e2e_runner(ready=True)
        for mode in ('all', 'examples'):
            for running in ('false', 'true'):
                for rc in ('0', '37'):
                    with self.subTest(mode=mode, running=running, rc=rc):
                        self.clear_podman_log()
                        result = self.run_e2e(mode, FAKE_RUNNING=running,
                            FAKE_EXISTS_RC='0', FAKE_CTEST_RC=rc)
                        self.assertEqual(result.returncode, int(rc), result.stderr)
                        log = self.log.read_text()
                        self.assertEqual(log.count('e2e up'), int(running == 'false'))
                        self.assertEqual(log.count('kube down'), 1)
                        self.assertLess(log.index('ctest '), log.index('kube down'))
                        self.assertIn('container exists', log)
                        self.assertEqual('-L examples' in log, mode == 'examples')

    def test_e2e_build_failure_stops_preexisting_stack(self):
        self.prepare_e2e_runner()
        (self.root / 'scripts/build.sh').write_text('#!/usr/bin/env bash\nexit 38\n')
        result = self.run_e2e(FAKE_RUNNING='true', FAKE_EXISTS_RC='0')
        self.assertEqual(result.returncode, 38, result.stderr)
        self.assertIn('kube down', self.log.read_text())

    def test_e2e_initial_workspace_is_private(self):
        self.prepare_e2e_runner()
        self.assertFalse(self.workspace.exists())
        (self.root / 'scripts/build.sh').write_text('#!/usr/bin/env bash\nexit 38\n')
        result = subprocess.run(
            ['bash', '-c', 'umask 022; exec bash "$1"', 'e2e',
             str(self.root / 'scripts/test-e2e.sh')], cwd=self.root,
            env=self.env | {'FAKE_RUNNING': 'true', 'FAKE_EXISTS_RC': '0'},
            text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 38, result.stderr)
        self.assertEqual(self.workspace.stat().st_mode & 0o777, 0o700)
        self.assertIn('kube down', self.log.read_text())

    def test_e2e_keep_flag_and_cleanup_failure_status(self):
        self.prepare_e2e_runner(ready=True)
        for keep, test_rc, down_rc, expected in [('1', '0', '0', 0),
                                                ('0', '0', '39', 1),
                                                ('0', '37', '39', 37)]:
            self.clear_podman_log()
            result = self.run_e2e(FAKE_RUNNING='true', FAKE_EXISTS_RC='0',
                LOCKDC_E2E_KEEP_DEVSERVICES=keep, FAKE_CTEST_RC=test_rc,
                FAKE_DOWN_RC=down_rc)
            self.assertEqual(result.returncode, expected, result.stderr)
            self.assertEqual('kube down' in self.log.read_text(), keep == '0')

    def start_waiting_e2e(self):
        Path(str(self.log) + '.ctest').unlink(missing_ok=True)
        process = subprocess.Popen(['bash', str(self.root / 'scripts/test-e2e.sh')],
            cwd=self.bin, env=self.env | {'FAKE_RUNNING': 'true', 'FAKE_EXISTS_RC': '0',
                                         'FAKE_CTEST_WAIT': '1'},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True)
        self.addCleanup(self.stop_waiting_e2e, process)
        deadline = time.monotonic() + 5
        while not Path(str(self.log) + '.ctest').exists():
            if process.poll() is not None or time.monotonic() > deadline:
                self.fail('E2E did not reach CTest')
            time.sleep(0.01)
        return process

    @staticmethod
    def stop_waiting_e2e(process):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
        process.communicate(timeout=10)

    def test_e2e_interrupt_stops_stack_and_preserves_signal_status(self):
        self.prepare_e2e_runner(ready=True)
        for sig, expected in [(signal.SIGTERM, 143), (signal.SIGINT, 130)]:
            self.clear_podman_log()
            process = self.start_waiting_e2e()
            os.killpg(process.pid, sig)
            _, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, expected, stderr)
            self.assertIn('kube down', self.log.read_text())

    def test_e2e_ctest_startup_interrupt_stops_stack(self):
        self.prepare_e2e_runner(ready=True)
        # Deliver INT to the mock CTest while its readiness command is still
        # running, then let that command succeed. Without an installed trap,
        # Bash can discard the interrupt and continue into the blocking wait.
        touch = self.bin / 'touch'
        touch.write_text(f'''#!/usr/bin/env bash
"{shutil.which('touch')}" "$@"
if [[ "$1" == *.ctest-starting ]]; then
  # This case deliberately interrupts before native readiness. Unblock the
  # test driver too, so it can assert the startup signal status and teardown.
  "{shutil.which('touch')}" "${{1%-starting}}"
  kill -INT "$PPID"
fi
''')
        touch.chmod(0o700)
        process = self.start_waiting_e2e()
        _, stderr = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 130, stderr)
        self.assertIn('kube down', self.log.read_text())

    def test_overlapping_e2e_is_rejected_without_tearing_down_active_stack(self):
        self.prepare_e2e_runner(ready=True)
        process = self.start_waiting_e2e()
        try:
            # Simulate the active run having selected disk-b as primary. A
            # rejected contender must not render disk-a over that decision,
            # or even truncate and rewrite identical manifest bytes.
            subprocess.run(['python3', str(self.root / 'scripts/render-devenv.py'),
                            '--disk-primary', 'b'], env=self.env, check=True,
                           capture_output=True)
            paths = [self.workspace / 'env.sh', self.workspace / 'devenv.yaml']
            before = [(p.read_bytes(), p.stat().st_mtime_ns) for p in paths]
            result = self.run_e2e(FAKE_RUNNING='true', FAKE_EXISTS_RC='0')
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('already running', result.stderr)
            self.assertNotIn('kube down', self.log.read_text())
            self.assertEqual([(p.read_bytes(), p.stat().st_mtime_ns) for p in paths],
                             before)
        finally:
            self.stop_waiting_e2e(process)
        self.assertEqual(self.log.read_text().count('kube down'), 1)

    def test_teardown_detects_leftover_pods_containers_and_inspection_errors(self):
        self.run_command('render')
        containers = self.manifest()['spec']['containers'] + self.manifest()['spec']['initContainers']
        for override in ([{'FAKE_POST_DOWN_POD_RC': '0'},
                          {'FAKE_POST_DOWN_POD_RC': '125'},
                          {'FAKE_POST_DOWN_CONTAINER_RC': '125'}] +
                         [{'FAKE_RESIDUAL_CONTAINER': c['name']} for c in containers]):
            self.clear_podman_log()
            result = self.run_command('reset', success=False, **override)
            self.assertTrue((self.workspace / 'devenv.yaml').exists())
            self.assertRegex(result.stderr, 'left .* behind|Cannot verify E2E teardown')

    def test_remote_benchmark_uses_the_other_node_when_disk_b_is_primary(self):
        self.run_command('render', LOCKDC_DISK_A_PORT='60441', LOCKDC_DISK_B_PORT='60442')
        subprocess.run(['python3', str(self.root / 'scripts/render-devenv.py'),
                        '--disk-primary', 'b'], env=self.env, check=True,
                       capture_output=True)
        shutil.copyfile(ROOT / 'Makefile', self.root / 'Makefile')
        fake = self.root / 'build/x86_64-linux-gnu-release/bench/lockdc_bench'
        fake.parent.mkdir(parents=True)
        fake.write_text('#!/usr/bin/env bash\nprintf "%s\\n" '
                        '"$LOCKDC_OUTBOX_BENCH_ENDPOINT" "$LOCKDC_OUTBOX_BENCH_FAILOVER_ENDPOINT"\n')
        fake.chmod(0o700)
        result = subprocess.run(['make', '--no-print-directory', '-s',
                                 '__benchmark-outbox-remote'], cwd=self.root,
                                env=self.env, text=True, capture_output=True,
                                timeout=10, check=True)
        self.assertEqual(result.stdout.splitlines(),
                         ['https://localhost:60442', 'https://localhost:60441'])

    def test_symlinked_state_is_rejected_without_writing_outside_workspace(self):
        self.workspace.mkdir(parents=True)
        outside = self.root / 'devenv'
        (self.workspace / 'credentials').symlink_to(outside, target_is_directory=True)
        self.run_command('render', success=False)
        self.assertFalse((outside / 'disk').exists())

    def test_unknown_placeholders_fail_before_service_creation(self):
        template = self.root / 'devenv.yaml.in'
        template.write_text(template.read_text().replace('@POD_NAME@', '@UNKNOWN@'))
        self.run_command('render', success=False)
        self.assertFalse((self.workspace / 'devenv.yaml').exists())
        self.assertFalse(self.log.exists())

if __name__ == '__main__':
    unittest.main()
