"""Run all repository offline suites in separate, socket-blocked processes."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SUITES = [
    'test_crypto.py', 'test_p1p7.py', 'test_merge.py', 'test_speed.py',
    'test_audit2fix.py', 'test_audit12.py', 'test_receiver_hardening.py',
    'test_media_login_hardening.py', 'test_delivery_hardening.py',
    'test_worker_state.py', 'test_burst_ownership.py', 'test_latency_trace.py',
]
GUARD = '''import os, runpy, socket, sys, tempfile
from pathlib import Path
root=Path(sys.argv[1]); target=Path(sys.argv[2]); state=sys.argv[3]
sys.path.insert(0,str(root))
def deny(*a,**kw): raise RuntimeError('offline suite forbids real sockets')
socket.socket.connect=deny; socket.socket.connect_ex=deny
socket.create_connection=deny; socket.getaddrinfo=deny
tempfile.tempdir=state
os.environ['WECHAT_BRIDGE_DIR']=state
import wechat_bridge.cli as cli
import wechat_bridge.store as store
notice=str(Path(state)/'fake-relogin-notified')
cli.RELOGIN_NOTIFIED_PATH=notice; store.RELOGIN_NOTIFIED_PATH=notice
sys.argv=[str(target)]
runpy.run_path(str(target),run_name='__main__')
'''

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args()
    output=args.output_dir or Path(tempfile.mkdtemp(prefix='wechat-offline-results-'))
    output.mkdir(parents=True,exist_ok=True)
    results=[]
    for suite in SUITES:
        with tempfile.TemporaryDirectory(prefix='wechat-offline-state-') as state:
            try:
                proc=subprocess.run([sys.executable,'-I','-c',GUARD,str(ROOT),
                    str(ROOT/'tests'/suite),state],cwd=ROOT,text=True,
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=120)
                text,code=proc.stdout,proc.returncode
            except subprocess.TimeoutExpired as error:
                text=error.stdout or b''
                if isinstance(text,bytes):text=text.decode('utf-8','replace')
                code='TIMEOUT'
        (output/(suite+'.log')).write_text(text)
        result={'suite':suite,'exit':code,'status':'PASS' if code==0 else 'FAIL'}
        results.append(result)
        print(json.dumps(result,ensure_ascii=False),flush=True)
        if code!=0:print('\n'.join(text.splitlines()[-24:]),flush=True)
    record={'network':'real sockets blocked','scope':'offline only; not Muse/WeChat acceptance',
            'sourceRoot':str(ROOT),'results':results}
    (output/'results.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
    print('Logs: '+str(output))
    return 0 if all(r['exit']==0 for r in results) else 1

if __name__=='__main__':raise SystemExit(main())
