import sys, time, threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, r'rx-rust-py/python')

# 手动实现一个简化版测试
def test_dispatch():
    num_workers = 2
    items = [1, 2, 3]
    
    executor = ThreadPoolExecutor(max_workers=num_workers)
    state_lock = threading.Lock()
    done_event = threading.Event()
    state = {
        "buffer": deque(),
        "active": 0,
        "closed": False,
        "errored": False,
        "completed_called": False,
    }
    results = []
    
    def worker_task(v):
        print(f'[worker] START v={v}, active_before={state["active"]}, buffer={list(state["buffer"])}')
        result = v * 10
        results.append(result)
        print(f'[worker] emit result={result}')
        
        should_finish = False
        with state_lock:
            state["active"] -= 1
            print(f'[worker] after decrement active={state["active"]}, closed={state["closed"]}, buffer_len={len(state["buffer"])}')
            if (state["closed"] and not state["errored"]
                    and not state["completed_called"]
                    and state["active"] == 0
                    and len(state["buffer"]) == 0):
                state["completed_called"] = True
                should_finish = True
        
        if should_finish:
            print('[worker] SETTING done_event!')
            done_event.set()
            return
        
        # try to submit next from buffer
        print(f'[worker] trying submit_next_from_buffer')
        submit_next_from_buffer()
        print(f'[worker] END v={v}')
    
    def submit_next_from_buffer():
        with state_lock:
            if state["errored"] or state["completed_called"]:
                print('  [submit] errored/completed, skip')
                return False
            if state["active"] >= num_workers or len(state["buffer"]) == 0:
                print(f'  [submit] active={state["active"]}>={num_workers} or buffer empty len={len(state["buffer"])}')
                return False
            value = state["buffer"].popleft()
            state["active"] += 1
            print(f'  [submit] popped {value}, active now={state["active"]}')
        
        print(f'  [submit] submitting task for {value}')
        executor.submit(worker_task, value)
        return True
    
    def pump_buffer():
        while True:
            if not submit_next_from_buffer():
                break
    
    # Simulate on_next
    for v in items:
        with state_lock:
            state["buffer"].append(v)
            print(f'[on_next] appended {v}, buffer={list(state["buffer"])}')
        pump_buffer()
    
    # on_completed
    with state_lock:
        state["closed"] = True
        print(f'[on_completed] closed=True, active={state["active"]}, buffer_len={len(state["buffer"])}')
        if state["active"] == 0 and len(state["buffer"]) == 0:
            state["completed_called"] = True
            done_event.set()
            print('[on_completed] set done_event immediately!')
    
    # Wait
    print('[main] Waiting for done_event...')
    finished = done_event.wait(timeout=5)
    print(f'[main] Finished={finished}, results={sorted(results)}')
    
    executor.shutdown(wait=False)

test_dispatch()
