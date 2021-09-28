# batch.py
import subprocess as sp
from pathlib import Path
import argparse
import os
import pandas as pd

def run(cmd_list):
    """
    Convenient subprocess wrapper to execute other OS processes 
    Args:
        cmd_list (list): list of args e.g. ['powershell.exe', 'copyitem', '"c:\abc.txt"', '-destination', '"d:\"']
    Returns:
        Subprocess CompletedProcess class: Includes information about state of process, stdout, etc.,
    """
    process = sp.run(cmd_list, check=True)
    return process

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Dissolving Segments Script'
        )
    parser.add_argument('-batch', type=str, help='batch file')
    parser.add_argument('-arcpy', type=str, help='batch file')
    
    args = parser.parse_args()
    arcpy_python_path = str(Path(args.arcpy))
    batch = pd.read_csv(args.batch)
    batch_records = batch.to_dict('records')
    aligment_script_path = str(Path(os.path.dirname(__file__)) / "dissolve_and_align_segments.py")
    
    for fname in batch_records:
        segments = Path(fname['segs'])
        out_segments = Path(fname['o_segs'])
        lc_raw = Path(fname['lc_raw'])
        lc_albers = Path(fname['lc_albers'])
        segs_aligned = Path(fname['aligned_segs'])
        segs_aligned = Path(fname['aligned_segs'])
        #windows pathlib format
        args = [segments, out_segments, lc_raw, lc_albers, segs_aligned, segs_aligned,]
        # convert to string
        args = [str(arg) for arg in args]

        cmd_list = [arcpy_python_path, aligment_script_path] + args
        
        result = run(cmd_list)
        
        
        result = run(cmd_list)
