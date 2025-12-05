# RUN Test Guild

- set PYTHONPATH as bellow
  ```bash
  export PYTHONPATH="${PYTHONPATH}:${falconfs_src_path}/cloud_native/falcon_cm"
  ```
- run test as bellow
  ```
  python3 -m unittest falcon_cm_test.py
  ```