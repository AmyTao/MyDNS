# Test flood test on a grid of parameters

import numpy as np
import os
import re
import itertools
import subprocess
import pandas as pd

unreliables = [True]
nclerks = list(range(3, 10, 2)) + list(range(11, 100, 10))
nservers = list(range(3, 10, 2)) + list(range(11, 100, 10))

# fixed value please
dnsduration = 10

# for testing override only
# nclerks = list(range(3, 10, 3))
# nservers = list(range(3, 10, 3))

pattern1 = re.compile(r"Total packets: (\d+), total put: (\d+), total query: (\d+)")
pattern2 = re.compile(r"Passed --\s+(\d+\.\d+)s,\s+(\d+) servers,\s+(\d+) rpcs")

ResultTable = pd.DataFrame(columns=["nclerk", "nservers", "unreliable",
                                    "total_packets", "total_put", "total_query", 
                                    "duration", "servers", "rpcs"]) 


for nclerk, nserver, unreliable in itertools.product(nclerks, nservers, unreliables):
    print(f"[INFO] nclerk: {nclerk}, nservers: {nserver}, unreliable: {unreliable}")
    
    try:
        delete_logs = subprocess.Popen(["bash", "./delete_logs.sh"], 
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        delete_logs.wait()

        dns_server = subprocess.Popen(["go", "run", "main.go", 
                                    "--nservers", str(nserver), 
                                    "--nclerks", str(nclerk),
                                    "--dnsDuration", str(dnsduration),
                                    ] + (["--unreliable"] if unreliable else []), 
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE
                                    )
        tester = subprocess.Popen(["python3", "client_tests/flood_test.py", 
                                "--nclients", str(nclerk)], 
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE
                                )

        # when dns server finishes, get the output
        retval = dns_server.wait()
        if retval != 0:
            print(dns_server.stdout.read().decode())
            print(dns_server.stderr.read().decode())

            raise Exception("dns server failed")

        # kill the tester
        tester.kill()
        if tester.returncode is not None and tester.returncode != 0:
            print(tester.stdout.read().decode())
            print(tester.stderr.read().decode())
            raise Exception("tester failed")        

        output = dns_server.stdout.read().decode()

        match1 = pattern1.search(output)
        match2 = pattern2.search(output)

        if match1 and match2:
            total_packets = int(match1.group(1))
            total_put = int(match1.group(2))
            total_query = int(match1.group(3))
            duration = float(match2.group(1))
            servers = int(match2.group(2))
            rpcs = int(match2.group(3))

            print(f"total_packets: {total_packets}, total_put: {total_put}, total_query: {total_query}, rpcs: {rpcs}.")
            
            # Add one row of result to the result table
            ResultTable.loc[len(ResultTable)] = [nclerk, nserver, unreliable,
                                                total_packets, total_put, total_query,
                                                duration, servers, rpcs]
        else:
            print(output)
            print(match1, match2)
            raise Exception("output not matched")
    except Exception as e:
        print(f'[ERROR] : We met exception at nclerk={nclerk}, nserver={nserver}')
        raise e
    finally:
        delete_logs.kill()
        dns_server.kill()
        tester.kill()

ResultTable.to_csv("flood_test_grid.csv", index=False)