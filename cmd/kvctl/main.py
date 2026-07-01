import sys
import requests

def print_usage():
    print("Usage:")
    print("  kvctl [options] GET <key>          - Get value for key")
    print("  kvctl [options] PUT <key> <value>  - Put key-value pair")
    print("  kvctl [options] DELETE <key>       - Delete key")
    print("  kvctl [options] STATUS             - Get node status")
    print("\nOptions:")
    print("  -addr string   HTTP address of the node (default \"localhost:8081\")")
    print("  -stale         Allow reading stale data from follower (GET only)")

def do_request(method, url, data=None):
    while True:
        try:
            resp = requests.request(method, url, data=data, allow_redirects=False, timeout=5.0)
        except Exception as e:
            print(f"HTTP request failed: {e}")
            sys.exit(1)

        # Handle redirects manually
        if resp.status_code in (307, 301, 302):
            try:
                redirect_data = resp.json()
                leader_url = redirect_data.get("leader")
                if leader_url:
                    url = leader_url
                    print(f"Redirecting to leader: {url}")
                    continue
            except Exception:
                pass

            loc = resp.headers.get("Location")
            if loc:
                url = loc
                print(f"Redirecting to leader (Location header): {url}")
                continue

            print(f"Redirect status received, but leader address could not be resolved. Body: {resp.text}")
            sys.exit(1)

        if resp.status_code != 200:
            print(f"Error [Status {resp.status_code}]: {resp.text}")
            sys.exit(1)

        print(resp.text.strip())
        return

def main():
    addr = "localhost:8081"
    stale = False
    args = sys.argv[1:]

    # Parse options manually to stay fully compatible with Go's CLI flag variations
    cmd_args = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--addr" or arg == "-addr":
            if i + 1 < len(args):
                addr = args[i+1]
                i += 2
            else:
                print("Error: missing value for addr")
                sys.exit(1)
        elif arg == "--stale" or arg == "-stale":
            stale = True
            i += 1
        elif arg.startswith("--addr="):
            addr = arg.split("=", 1)[1]
            i += 1
        elif arg.startswith("-addr="):
            addr = arg.split("=", 1)[1]
            i += 1
        else:
            cmd_args.append(arg)
            i += 1

    if not cmd_args:
        print_usage()
        sys.exit(1)

    cmd = cmd_args[0].upper()
    url = addr
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "http://" + url

    if cmd == "GET":
        if len(cmd_args) < 2:
            print("Usage: kvctl GET <key>")
            sys.exit(1)
        key = cmd_args[1]
        req_url = f"{url}/kv/{key}"
        if stale:
            req_url += "?stale=true"
        do_request("GET", req_url)

    elif cmd == "PUT":
        if len(cmd_args) < 3:
            print("Usage: kvctl PUT <key> <value>")
            sys.exit(1)
        key = cmd_args[1]
        val = cmd_args[2]
        req_url = f"{url}/kv/{key}"
        do_request("PUT", req_url, data=val.encode("utf-8"))

    elif cmd == "DELETE":
        if len(cmd_args) < 2:
            print("Usage: kvctl DELETE <key>")
            sys.exit(1)
        key = cmd_args[1]
        req_url = f"{url}/kv/{key}"
        do_request("DELETE", req_url)

    elif cmd == "STATUS":
        req_url = f"{url}/status"
        do_request("GET", req_url)

    else:
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()
