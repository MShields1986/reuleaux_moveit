#!/usr/bin/env python3
"""
Compare two reachability map HDF5 files to identify differences in reachable
sphere/pose coverage. Useful for debugging discrepancies between generation methods.

Usage:
  python3 compare_reachability_maps.py <file_a> <file_b>
  python3 compare_reachability_maps.py  # uses hard-coded serial vs concurrent defaults
"""
import sys
import h5py
import numpy as np

MAPS_DIR = '/home/matthew/Desktop/phd_workspaces/reuleaux_moveit/catkin_ws/src/reuleaux_moveit/map_generation/maps/'

def load_map(path):
    with h5py.File(path, 'r') as f:
        spheres = f['Spheres/sphere_dataset'][:]   # (N, 4): x y z ri
        poses   = f['Poses/poses_dataset'][:]      # (M, 10): x y z qx qy qz qw j1 j2 ...
    return spheres, poses

def summarise(name, spheres, poses):
    print(f'=== {name} ===')
    print(f'  Reachable spheres : {len(spheres)}')
    print(f'  Reachable poses   : {len(poses)}')
    if len(spheres):
        ri = spheres[:, 3]
        print(f'  RI range          : {ri.min():.2f} – {ri.max():.2f}  mean={ri.mean():.2f}')
        print(f'  X range           : {spheres[:,0].min():.3f} – {spheres[:,0].max():.3f}')
        print(f'  Y range           : {spheres[:,1].min():.3f} – {spheres[:,1].max():.3f}')
        print(f'  Z range           : {spheres[:,2].min():.3f} – {spheres[:,2].max():.3f}')

def sphere_key(row, decimals=3):
    return tuple(np.round(row[:3], decimals))

def compare(name_a, spheres_a, name_b, spheres_b, tol=0.01):
    keys_a = {sphere_key(s): s[3] for s in spheres_a}
    keys_b = {sphere_key(s): s[3] for s in spheres_b}

    only_a = {k: v for k, v in keys_a.items() if k not in keys_b}
    only_b = {k: v for k, v in keys_b.items() if k not in keys_a}
    both   = {k: (keys_a[k], keys_b[k]) for k in keys_a if k in keys_b}

    print(f'\n=== Comparison: {name_a}  vs  {name_b} ===')
    print(f'  Spheres in both   : {len(both)}')
    print(f'  Only in {name_a:12s}: {len(only_a)}')
    print(f'  Only in {name_b:12s}: {len(only_b)}')

    if both:
        print(f'\n  Spheres present in both (x,y,z) → ri_{name_a} / ri_{name_b}:')
        for k, (ra, rb) in sorted(both.items()):
            print(f'    {k}  →  {ra:.2f} / {rb:.2f}')

    if only_a:
        print(f'\n  Spheres ONLY in {name_a}:')
        for k, ri in sorted(only_a.items()):
            print(f'    {k}  ri={ri:.2f}')

    if only_b:
        print(f'\n  Spheres ONLY in {name_b}:')
        for k, ri in sorted(only_b.items()):
            print(f'    {k}  ri={ri:.2f}')

if __name__ == '__main__':
    if len(sys.argv) == 3:
        path_a, path_b = sys.argv[1], sys.argv[2]
        name_a = path_a.split('/')[-1].replace('.h5','')
        name_b = path_b.split('/')[-1].replace('.h5','')
    else:
        path_a = MAPS_DIR + 'kmriiwa_tooled_ec_contact_0.8_reachability_serial.h5'
        path_b = MAPS_DIR + 'kmriiwa_tooled_ec_contact_0.8_reachability_concurrent.h5'
        name_a, name_b = 'serial', 'concurrent'

    spheres_a, poses_a = load_map(path_a)
    spheres_b, poses_b = load_map(path_b)

    summarise(name_a, spheres_a, poses_a)
    print()
    summarise(name_b, spheres_b, poses_b)

    compare(name_a, spheres_a, name_b, spheres_b)
