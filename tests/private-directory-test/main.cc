#include "dfs.h"
#include "DBarrier.h"

using namespace std;

string root_dir;
int files_per_dir;
int thread_num;
int wait_time;
int client_cache_size;
int loop;
int client_id;
int mount_per_client;
int wait_port = 1111;
int file_size = 65536;
int client_number = 1;

int file_num = 0;
int cpu_count;

struct timespec start_time;
int round_idx = 0;
atomic<int> finished_thread_count(0);

double last_elapsed_time = 0;
uint64_t last_op_done = 0, last_latency_done = 0;

DBarrierOnUDP *g_barrier;

volatile uint64_t op_count[16384];
volatile uint64_t latency_count[16384];

void (*workloads[])(string, int) = {workload_init, workload_create, workload_stat, workload_open, workload_close, workload_delete, workload_mkdir, workload_rmdir, workload_open_write_close, workload_open_write_close_nocreate, workload_open_read_close, workload_kv_put, workload_kv_get, workload_kv_del, workload_slice_put, workload_slice_get, workload_slice_del, workload_uninit};

void init_namespace() {
  int round_num = sizeof(workloads) / sizeof(void (*)());
  if (round_idx == 0 || round_idx == round_num - 1) {
    files_per_dir = 1 + thread_num;
    thread_num = 1;
  }

  file_num = thread_num * files_per_dir;
}

void print_stat() {
  struct timespec end_time;
  uint64_t op_done = 0, latency_done = 0;
  for (int i = 0; i < thread_num; i++) {
    op_done += op_count[i];
    latency_done += latency_count[i];
  }
  clock_gettime(CLOCK_MONOTONIC, &end_time);
  double elapsed_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

  double throughput = (double)(op_done - last_op_done) / (elapsed_time - last_elapsed_time);
  double avg_latency = (double)(latency_done - last_latency_done) / (op_done - last_op_done);

  cout << fmt::format("Round {}, Time {}, OPs {}, Current Throughput {}, Current Average Latency {}", round_idx, elapsed_time, op_done, throughput, avg_latency) << std::endl;

  last_elapsed_time = elapsed_time;
  last_op_done = op_done;
  last_latency_done = latency_done;
}

void print_final_stat() {
  struct timespec end_time;
  uint64_t op_done = 0, latency_done = 0;
  for (int i = 0; i < thread_num; i++) {
    op_done += op_count[i];
    latency_done += latency_count[i];
  }
  clock_gettime(CLOCK_MONOTONIC, &end_time);

  double elapsed_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
  double avg_latency = (double)latency_done / op_done;
  cout << fmt::format("[FINISH] Round {}, Time {}, OPs {}, Throughput {}, Average Latency {}", round_idx, elapsed_time, op_done, (double)op_done / elapsed_time, avg_latency) << std::endl;
}

auto thread_func = [](int thread_id) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  int bind_cpu = ((cpu_count / mount_per_client) * (wait_port - 1111) +  thread_id) % cpu_count;
  CPU_SET(bind_cpu, &cpus);
  
  if(sched_setaffinity(0, sizeof(cpu_set_t), &cpus) == -1)
  {
      printf("warning: could not set CPU affinity, continuing...\n");
  }

  string new_path = fmt::format("{}client_{}_{}/", root_dir, client_id, wait_port);

  g_barrier->wait();

  workloads[round_idx](new_path, thread_id);

  finished_thread_count.fetch_add(1);
  if (finished_thread_count.load() == thread_num) {
    print_final_stat();
  }
  return 0;
};

int main(int argc, char **argv) {

  if (argc != 11) {
    cerr << "Usage: " << argv[0] << " <ROOT DIR (end with /)> <FILES PER DIR> <THREAD NUMBER> <ROUND INDEX> <CLIENT ID> <MOUNT_PER_CLIENT> <CLIENT CACHE SIZE> <WAIT PORT> <FILE SIZE> <CLIENT NUMBER>" << std::endl;
    return -1;
  }

  cout << "Warning: integer overflows if directory tree is too large (more than 2 billion directories)" << std::endl;

  cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
  if (cpu_count < 1) {
    perror("sysconf");
    exit(EXIT_FAILURE);
  }
  cout << "CPU count: " << cpu_count << std::endl;

  root_dir = argv[1];
  files_per_dir = atoi(argv[2]);
  thread_num = atoi(argv[3]);
  round_idx = atoi(argv[4]);
  client_id = atoi(argv[5]);
  mount_per_client = atoi(argv[6]);
  client_cache_size = atoi(argv[7]);
  wait_port = atoi(argv[8]);
  file_size = atoi(argv[9]);
  client_number = atoi(argv[10]);
  thread_num *= client_number;

  g_barrier = new DBarrierOnUDP(wait_port);

  init_namespace();

  // for (int i = 0; i < file_num; i++) {
  //   std::cout << filepaths[i] << std::endl;
  // }
  // return 0;

  // init client
  int ret = dfs_init(client_number);
  if (ret != 0) {
    return ret;
  }

  cout << "Start opening files" << std::endl;
  cout << "Thread number * client number: " << thread_num << std::endl;
  cout << "Wait time: " << wait_time << "s" << std::endl;
  cout << "File num: " << file_num << std::endl;

  memset((void *)op_count, 0, sizeof(op_count));
  memset((void *)latency_count, 0, sizeof(latency_count));
  finished_thread_count.store(0);
  last_elapsed_time = 0;
  last_op_done = 0;
  g_barrier->reset();

  vector<thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(thread_func, i);
  }

  g_barrier->wait();

  clock_gettime(CLOCK_MONOTONIC, &start_time);
  int time_waited = 0;
  while (1) {
    this_thread::sleep_for(chrono::seconds(SLEEP_TIME));
    time_waited += SLEEP_TIME;
    if (finished_thread_count.load() == thread_num)
      break;
    print_stat();
  }

  for (auto &t : threads) {
    t.join();
  }
  
  dfs_shutdown();
}