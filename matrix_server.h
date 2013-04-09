#ifndef MATRIX_SERVER_H_
#define MATRIX_SERVER_H_

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <sstream>
#include <math.h>
#include <errno.h>
#include <algorithm>
#include "cpp_zhtclient.h"
#include "matrix_client.h"
#include "matrix_util.h"
#include <queue>
#include <deque>
#include <vector>
#include <map>
#include <set>
#include <climits>

#define SIXTY_KILOBYTES 61440
#define STRING_THRESHOLD SIXTY_KILOBYTES

#define MAX_LOAD INT_MAX
#define NUM_COMPUTE_SLOTS 100

#define INITIAL_WAIT_TIME 1000
#define WAIT_THRESHOLD 1000000

typedef deque<TaskQueue_Item*> TaskQueue;	// Queue to hold tasks
typedef deque<string> NodeList;			// List to hold node-ids to be notified of any operation
typedef pair<string, vector<string> > ComPair;	// Pair  <TaskID and List of nodes to be notified>
typedef deque<ComPair> CompQueue;		// Queue to hold tasks that have finished executing

//typedef pair<uint32_t, int32_t> Server_Slots;	// Pair <Server ID and Available slots at that server>
//typedef list<Server_Slots> ServerSlots;		// list of Server ID and Available slots at that server

typedef map<uint32_t, int32_t> ServerSlots;	// Map Server ID ---> Available slots at that server
typedef map<string, ServerSlots> ComputeNodesInfo;// Map Task ---> list<Server ID ---> Available slots at that server>
typedef map<uint32_t, bool> ComputeNodesStatus;	// Map Compute Slot ---> free/busy
typedef list<uint32_t> SlotList;		// List of compute slots
typedef map<string, SlotList> LockComputeNodes;	// Map Task ID ---> List of compute slots to lock for that task

//typedef list<TaskQueue_Item*> TaskQueue;
//typedef list<string> NodeList;

using namespace std;

/*struct CompareSecond
{
	bool operator()(const NeighPair& firstPair, const NeighPair& secondPair) {
		return firstPair.second < secondPair.second;
	}
};*/

class Worker {
public:

	Worker();
	Worker(char *parameters[], NoVoHT *novoht);
	virtual ~Worker();

	string ip;

	int num_nodes;				// Number of computing nodes of Matrix
	int num_cores;				// Number of cores of a node
	int num_idle_cores;			// Number of idle cores of a node
	char neigh_mode;			// neighbors selection mechanism: 'static' or 'dynamic random'
	int num_neigh;				// Number of neighbors of a node
	//int *neigh_index;			// Array to hold neighbors index for which to poll load
	//NeighMap neighmap;			// Neighbors --> Load mapping
	int least_loaded_node;			// Neighbor index from which to steal task
	int selfIndex;				// Self Index in the membership list
	long poll_interval;			// The poll interval value for work stealing
	long poll_threshold;			// Threshold beyond which polling interval should not double
	ZHTClient svrclient;			// for server to server communication
	
	uint32_t freeLockedSlots(string &task_id);
	int32_t freeLockedSlots(ComputeNodesInfo &availSlots, string &task_id);
	uint32_t getHalfSlots(string &task_id);
	ComputeNodesInfo getAvailSlotsInfo(uint32_t &numAvailSlots, string &task_id, uint32_t &reqdSlots);
	bool checkIfReqdSlotsAvail(TaskQueue_Item *qi, uint32_t &numAvailSlots);

	int32_t get_load_map();
	void chooseNeighbours(ComputeNodesInfo &availSlots, string &task_id);
	int32_t stealSlots(ComputeNodesInfo &availSlots, string &task_id);
	int32_t getComputeNodesInfo(ComputeNodesInfo &availSlots, string &task_id);
	int32_t executeTask(TaskQueue_Item **qi, ComputeNodesInfo &availSlots);
	int32_t get_load_info();
	int32_t get_compute_slots_info();
	int32_t get_monitoring_info();
	int32_t get_numtasks_to_steal();

	int checkIfTaskIsReady(string key);
	int moveTaskToReadyQueue(TaskQueue_Item **qi);

	int notify(ComPair &compair);

	string zht_lookup(string key);
	int zht_insert(string str);
	int zht_remove(string key);
	int zht_append(string str);
	int zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid);

	int update(Package &package);
	int update_nodehistory(uint32_t currnode, string alltasks);
	int update_numwait(string alltasks);

	map<uint32_t, NodeList> get_map(TaskQueue &queue);
	map<uint32_t, NodeList> get_map(vector<string> &mqueue);
};

extern Worker *worker;
extern int ON;

extern ofstream fin_fp;
extern ofstream log_fp;
extern long msg_count[10];

extern long task_comp_count;

//extern queue<string*> insertq;
extern queue<string*> waitq;
extern queue<string*> insertq_new;
extern queue<int*> migrateq;
extern bitvec migratev;

extern queue<string*> notifyq;

//extern pthread_mutex_t iq_lock;
extern pthread_mutex_t waitq_lock;
extern pthread_mutex_t iq_new_lock;
extern pthread_mutex_t mq_lock;
extern pthread_mutex_t notq_lock;

void* checkWaitQueue(void* args);
void* checkReadyQueue(void* args);
void* executeThread(void* args);
void* checkCompleteQueue(void* args);

void* HB_insertQ(void* args);
void* HB_insertQ_new(void* args);
void* HB_localinsertQ(void* args);
void* migrateTasks(void* args);
void* notQueue(void* args);

void backoff();
void resetwaitInterval();
bool eraseCondition(TaskQueue_Item *qi);

vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count);

extern pthread_attr_t attr;
#endif 
