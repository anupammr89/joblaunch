
#include "matrix_server.h"

Worker::Worker() {
	
}

Worker::~Worker() {

}

static LockComputeNodes lockedNodes;
static ComputeNodesStatus nodeStatus;

TaskQueue wqueue;
TaskQueue rqueue;
TaskQueue mqueue;
CompQueue cqueue;
static pthread_mutex_t w_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "wait queue"
static pthread_mutex_t r_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "ready queue"
static pthread_mutex_t c_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "complete queue"
static pthread_mutex_t m_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "migrate queue"
static pthread_mutex_t mutex_idle = PTHREAD_MUTEX_INITIALIZER;          // Lock for the "num_idle_cores"
static pthread_mutex_t mutex_finish = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "finish file"

static pthread_mutex_t zht_lock = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "zht"

//queue<string*> insertq;
queue<string*> waitq;
queue<string*> insertq_new;
queue<int*> migrateq;
bitvec migratev;
queue<string*> notifyq;

//pthread_mutex_t iq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t waitq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t iq_new_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t notq_lock = PTHREAD_MUTEX_INITIALIZER;

struct package_thread_args {
	queue<string*> *source;
	TaskQueue *dest;
	pthread_mutex_t *slock;
	pthread_mutex_t *dlock;
	Worker *worker;
};

static pthread_mutex_t msg_lock = PTHREAD_MUTEX_INITIALIZER;

ofstream fin_fp;
ofstream log_fp;
long msg_count[10];

int ON = 1;
long task_comp_count = 0;

uint32_t nots = 0, notr = 0;

uint32_t waitInterval = INITIAL_WAIT_TIME;
uint32_t waitThreshold = WAIT_THRESHOLD;

int work_steal_signal = 1;

//Worker *worker;
static NoVoHT *pmap;

pthread_attr_t attr; // thread attribute
static ofstream worker_start;
static ofstream task_fp;
static ofstream load_fp;
static ofstream migrate_fp;

/* filenames */
static string file_worker_start;	
static string file_task_fp;			
static string file_migrate_fp;		
static string file_fin_fp;			
static string file_log_fp;		

Worker::Worker(char *parameters[], NoVoHT *novoht) {
        /* set thread detachstate attribute to DETACHED */
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM);
        /* filename definitions */
        set_dir(parameters[9],parameters[10]);
        file_worker_start.append(shared);       file_worker_start.append("startinfo");
        file_task_fp.append(prefix);            file_task_fp.append("pkgs");
        file_migrate_fp.append(prefix);         file_migrate_fp.append("log_migrate");
        file_fin_fp.append(prefix);             file_fin_fp.append("finish");
        file_log_fp.append(prefix);             file_log_fp.append("log_worker");

        pmap = novoht;
        Env_var::set_env_var(parameters);
        svrclient.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
        //svrzht.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
        //svrmig.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);

        if(set_ip(ip)) {
                printf("Could not get the IP address of this machine!\n");
                exit(1);
        }

        for(int i = 0; i < 10; i++) {
                msg_count[i] = 0;
        }

        num_nodes = svrclient.memberList.size();
        num_cores = NUM_COMPUTE_SLOTS;
        num_idle_cores = num_cores;
        neigh_mode = 'd';
        //worker.num_neigh = (int)(sqrt(worker.num_nodes));
        num_neigh = (int)(log(num_nodes)/log(2));
        //neigh_index = new int[num_neigh];
        selfIndex = getSelfIndex(ip, atoi(parameters[1]), svrclient.memberList);        // replace "localhost" with proper hostname, host is the IP in C++ string
        ostringstream oss;
        oss << selfIndex;

        string f1 = file_fin_fp;
        f1 = f1 + oss.str();
        fin_fp.open(f1.c_str(), ios_base::app);

	if(LOGGING) {

                string f2 = file_task_fp;
                f2 = f2 + oss.str();
                task_fp.open(f2.c_str(), ios_base::app);

                string f3 = file_log_fp;
                f3 = f3 + oss.str();
                log_fp.open(f3.c_str(), ios_base::app);

                string f4 = file_migrate_fp;
                f4 = f4 + oss.str();
                migrate_fp.open(f4.c_str(), ios_base::app);
        }

        migratev = bitvec(num_nodes);

        srand((selfIndex+1)*(selfIndex+5));
        int rand_wait = rand() % 20;
        cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << endl;
        //cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << " going to wait for " << rand_wait << " seconds" << endl;
        sleep(rand_wait);

        file_worker_start.append(oss.str());
        string cmd("touch ");
        cmd.append(file_worker_start);
        executeShell(cmd);

        worker_start.open(file_worker_start.c_str(), ios_base::app);
        if(worker_start.is_open()) {
                worker_start << ip << ":" << selfIndex << " " << std::flush;
                worker_start.close();
                worker_start.open(file_worker_start.c_str(), ios_base::app);
        }


        int err;
        /*pthread_t *ready_queue_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
        pthread_create(ready_queue_thread, &attr, check_ready_queue, NULL);*/
	try {
        /*pthread_t *ready_queue_thread = new pthread_t[num_cores];
        for(int i = 0; i < num_cores; i++) {
                err = pthread_create(&ready_queue_thread[i], &attr, check_ready_queue, (void*) this);
                if(err){
                        printf("work_steal_init: pthread_create: ready_queue_thread: %s\n", strerror(errno));
                        exit(1);
                }
        }*/

	pthread_t *ready_queue_thread = new pthread_t();
	err = pthread_create(ready_queue_thread, &attr, checkReadyQueue, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: ready_queue_thread: %s\n", strerror(errno));
                exit(1);
        }

        pthread_t *wait_queue_thread = new pthread_t();
        err = pthread_create(wait_queue_thread, &attr, checkWaitQueue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: wait_queue_thread: %s\n", strerror(errno));
                exit(1);
        }

        pthread_t *complete_queue_thread = new pthread_t();
        /*err = pthread_create(complete_queue_thread, &attr, checkCompleteQueue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: complete_queue_thread: %s\n", strerror(errno));
                exit(1);
        }*/

        package_thread_args rq_args, wq_args;
        rq_args.source = &insertq_new;  wq_args.source = &waitq;
        rq_args.dest = &rqueue;         wq_args.dest = &wqueue;
        rq_args.slock = &iq_new_lock;   wq_args.slock = &waitq_lock;
        rq_args.dlock = &r_lock;          wq_args.dlock = &w_lock;
        rq_args.worker = this;          wq_args.worker = this;
        pthread_t *waitq_thread = new pthread_t();
        err = pthread_create(waitq_thread, &attr, HB_insertQ_new, (void*) &wq_args);
        if(err){
                printf("work_steal_init: pthread_create: waitq_thread: %s\n", strerror(errno));
                exit(1);
        }

        pthread_t *readyq_thread = new pthread_t();
        err = pthread_create(readyq_thread, &attr, HB_insertQ_new, (void*) &rq_args);
        if(err){
                printf("work_steal_init: pthread_create: ready_thread: %s\n", strerror(errno));
                exit(1);
        }

        pthread_t *migrateq_thread = new pthread_t();
        /*err = pthread_create(migrateq_thread, &attr, migrateTasks, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: migrateq_thread: %s\n", strerror(errno));
                exit(1);
        }*/

	pthread_t *notq_thread = new pthread_t();
        err = pthread_create(notq_thread, &attr, notQueue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: notq_thread: %s\n", strerror(errno));
                exit(1);
        }

        int min_lines = svrclient.memberList.size();
        string filename(file_worker_start);

        string cmd2("ls -l ");  cmd2.append(shared);    cmd2.append("startinfo*");      cmd2.append(" | wc -l");
        string result = executeShell(cmd2);
        //cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
        while(atoi(result.c_str()) < min_lines) {
                sleep(2);
                 //cout << "server: " << worker.selfIndex << " minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
                result = executeShell(cmd2);
        }
        //cout << "server: " << selfIndex << " minlines = " << min_lines << " cmd = " << cmd2 << " result = " << result << endl;

	pthread_t *resource_steal_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
        /*err = pthread_create(resource_steal_thread, &attr, resourcesteal, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: resource_steal_thread: %s\n", strerror(errno));
                exit(1);
        }*/

        delete ready_queue_thread;
        delete wait_queue_thread;
        delete complete_queue_thread;
        delete resource_steal_thread;
        delete readyq_thread;
        delete waitq_thread;
        delete migrateq_thread;
        delete notq_thread;
        }
	catch (std::bad_alloc& exc) {
                cout << "work_steal_init: failed to allocate memory while creating threads" << endl;
                exit(1);
        }
}

// parse the input string containing two delimiters
vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count) {
	vector< vector<string> > token_vector;
	stringstream whole_stream(input);
	num_vector = 0; per_vector_count = 0;
	string perstring;
	while(getline(whole_stream, perstring, delim1)) { //cout << "pertask = " << pertask << endl;
		num_vector++;
                vector<string> per_vector;
                size_t prev = 0, pos;
		while ((pos = perstring.find_first_of(delim2, prev)) != string::npos) {
                       	if (pos > prev) {
				try {
                               		per_vector.push_back(perstring.substr(prev, pos-prev));
				}
				catch (exception& e) {
					cout << "tokenize: (prev, pos-prev) " << " " << e.what() << endl;
					exit(1);
				}
                       	}
              		prev = pos+1;
               	}
               	if (prev < perstring.length()) {
			try {
                       		per_vector.push_back(perstring.substr(prev, string::npos));
			}
			catch (exception& e) {
                       	        cout << "tokenize: (prev, string::npos) " << " " << e.what() << endl;
                               	exit(1);
                        }
               	}
		try {
			token_vector.push_back(per_vector);
		}
		catch (exception& e) {
                        cout << "tokenize: token_vector.push_back " << " " << e.what() << endl;
                	exit(1);
                }
	}
	if(token_vector.size() > 0) {
		per_vector_count = token_vector.at(0).size();
	}
	return token_vector;
}

void* notQueue(void* args) {
	Worker *worker = (Worker*)args;
	string *st;
	Package package;

	while(ON) {
		while(notifyq.size() > 0) {
			pthread_mutex_lock(&notq_lock);
			if(notifyq.size() > 0) {
				try {
				st = notifyq.front();
				notifyq.pop();
				}
				catch (exception& e) {
					cout << "void* notifyq: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(&notq_lock);
                                continue;
                        }
			pthread_mutex_unlock(&notq_lock);
			package.ParseFromString(*st);
			delete st;
			worker->update(package);
		}
	}
}

// Insert tasks into queue without repeated fields
//int32_t Worker::HB_insertQ_new(NoVoHT *map, Package &package) {
void* HB_insertQ_new(void* args) {

	package_thread_args targs = *((package_thread_args*)args);
	queue<string*> *source = targs.source;
	TaskQueue *dest = targs.dest;
	pthread_mutex_t *slock = targs.slock;
	pthread_mutex_t *dlock = targs.dlock;
	Worker *worker = targs.worker;

	string *st;
	Package package;

	while(ON) {
		while(source->size() > 0) {
			pthread_mutex_lock(slock);
			if(source->size() > 0) {
				try {
				st = source->front();
				source->pop(); //cout << "recd something" << endl;
				}
				catch (exception& e) {
					cout << "void* HB_insertQ_new: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(slock);
                                continue;
                        }
			pthread_mutex_unlock(slock);
			package.ParseFromString(*st);
			delete st;

		        TaskQueue_Item *qi;

			uint32_t task_recd_count = 0;
		        string alltasks(package.realfullpath());
			int num_vector_count, per_vector_count;
		        vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
			//cout << "num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
			task_recd_count = num_vector_count;
			for(int i = 0; i < num_vector_count; i++) {
				qi = new TaskQueue_Item();
				try {
                			qi->task_id = tokenize_string.at(i).at(0); //cout << "insertq: qi->task_id = " << qi->task_id << endl;
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                                	exit(1);
                                }
                                try {
                                        qi->task_desc = tokenize_string.at(i).at(1);
                                }
                                catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(1)) " << " " << e.what() << endl;
                                        exit(1);
                                }
				stringstream num_slots_ss;
				try {
			                num_slots_ss << tokenize_string.at(i).at(2);
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(2)) " << " " << e.what() << endl;
                                        exit(1);
                                }
                		num_slots_ss >> qi->num_slots;

                		if(LOGGING) {
                                	task_fp << " taskid = " << qi->task_id;
					task_fp << " task_desc = " << qi->task_desc;
                                	task_fp << " num slots = " << qi->num_slots;
                		}
				
				pthread_mutex_lock(dlock);
				try {
		                        dest->push_back(qi);
                		}
		                catch (std::bad_alloc& exc) {
                		        cout << "HB_InsertQ_new: cannot allocate memory while adding element to ready queue" << endl;
		                        pthread_exit(NULL);
                		}
		                pthread_mutex_unlock(dlock);
		        }
			if(LOGGING) {
				log_fp << "Num tasks received = " << task_recd_count << " Queue length = " << dest->size() << endl;
			}
		}
	}
}

/*
//int32_t Worker::HB_localinsertQ(NoVoHT *map, Package &r_package) {
void* HB_localinsertQ(void* args) {


	Worker *worker = (Worker*)args;

	string *st;
        Package r_package;
	try {
	st = reinterpret_cast<string*>(args);
        r_package.ParseFromString(*st);
	}
	catch (exception& e) {
                cout << "void* HB_localinsertQ: " << e.what() << endl;
        	exit(1);
        }
	int numSleep = r_package.mode();
	int num_tasks = r_package.num();
	string clientid = r_package.realfullpath();

	char task[10];
        memset(task, '\0', 10);
        sprintf(task, "%d", numSleep);

	timespec start_tasks, end_tasks;
	int num_packages = 0;
        static int id = 1;

	TaskQueue_Item *qi;
        
        int total_submitted1 = 0;
        //clock_gettime(CLOCK_REALTIME, &start_tasks);
	for(int j = 0; j < num_tasks; j++) {
		qi = new TaskQueue_Item();
		qi->task_id = id++;
		qi->client_id = clientid;
		qi->task_description = task;
		timespec sub_time;
		clock_gettime(CLOCK_REALTIME, &sub_time);
		qi->submission_time = (uint64_t)sub_time.tv_sec * 1000000000 + (uint64_t)sub_time.tv_nsec;
		qi->num_moves = 0;
		pthread_mutex_lock(&lock);
		//ready_queue->add_element(&qi);
		try {
			rqueue.push_back(qi);
		}
		catch (std::bad_alloc& exc) {
                        cout << "HB_localInsertQ: cannot allocate memory while adding element to ready queue" << endl;
                 	pthread_exit(NULL);
                }
		pthread_mutex_unlock(&lock);
	}
	//clock_gettime(CLOCK_REALTIME, &end_tasks);

        //timespec diff = timediff(start_tasks, end_tasks);
        //cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;

	//if (ready_queue->get_length() == 1024000) {
	if(rqueue.size() == 1024000) {
        	//cout << "TaskQueue_Item = " << sizeof(TaskQueue_Item) << " TaskQueue = " << sizeof(TaskQueue) << endl;
		cout << "TaskQueue_Item = " << sizeof(TaskQueue_Item) << " TaskQueue = " << sizeof(rqueue) << endl;
        }
	//cout << "Worker:" << worker->selfIndex << " QueueLength: = " <<  ready_queue->get_length() << endl;
	//return ready_queue->get_length();
	//return rqueue.size();
	//delete qa;
	delete st;
}
*/

map<uint32_t, NodeList> Worker::get_map(TaskQueue &mqueue) {
	map<uint32_t, NodeList> update_map;
	/*Package package;
	package.set_operation(operation);
	if(operation == 25) {
		package.set_currnode(toid);
	}*/
	uint32_t num_nodes = svrclient.memberList.size();
	for(TaskQueue::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
		uint32_t serverid = myhash(((*it)->task_id).c_str(), num_nodes);
		string str((*it)->task_id); str.append("\'");
		if(update_map.find(serverid) == update_map.end()) {
			str.append("\"");
			NodeList new_list;
			new_list.push_back(str);
			update_map.insert(make_pair(serverid, new_list));
		}
		else {
			NodeList &exist_list = update_map[serverid];
			string last_str(exist_list.back());
			if((last_str.size() + str.size()) > STRING_THRESHOLD) {
				str.append("\"");
				exist_list.push_back(str);
			}
			else {
				exist_list.pop_back();
				str.append(last_str);
				exist_list.push_back(str);
			}
		}
	}
	return update_map;
}

map<uint32_t, NodeList> Worker::get_map(vector<string> &mqueue) {
        map<uint32_t, NodeList> update_map;
        /*Package package;
        package.set_operation(operation);
        if(operation == 25) {
                package.set_currnode(toid);
        }*/
        uint32_t num_nodes = svrclient.memberList.size();
        for(vector<string>::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
                uint32_t serverid = myhash((*it).c_str(), num_nodes);
                string str(*it); str.append("\'");
                if(update_map.find(serverid) == update_map.end()) {
                        str.append("\"");
                        NodeList new_list;
                        new_list.push_back(str);
                        update_map.insert(make_pair(serverid, new_list));
                }
                else {
                        NodeList &exist_list = update_map[serverid];
                        string last_str(exist_list.back());
                        if((last_str.size() + str.size()) > STRING_THRESHOLD) {
                                str.append("\"");
                                exist_list.push_back(str);
                        }
                        else {
                                exist_list.pop_back();
                                str.append(last_str);
                                exist_list.push_back(str);
                        }
                }
        }
        return update_map;
}

// pack the jobs into multiple packages - 2000 jobs per package
// and insert it into the ready queue of server that requested to steal tasks
//int Worker::migrateTasks(int num_tasks, ZHTClient &clientRet, int index){
void* migrateTasks(void *args) {

	Worker *worker = (Worker*)args;
	int index;
    while(ON) {
        //while(migrateq.size() > 0) {
	while(migratev.any()) {
			pthread_mutex_lock(&mq_lock);
			if(migratev.any()) {
				//int *index = (int*)args;                
                		//index = migrateq.front();
				index = migratev.pop();
                		//migrateq.pop();
				//cout << "1 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			}		        
			else {
				//cout << "migratev count = " << migratev.count() << endl;
				pthread_mutex_unlock(&mq_lock);
				continue;
			}
			if(index < 0 || index >= worker->num_nodes) {
				//cout << "bad index: worker = " << worker->selfIndex << " to index = " << index << endl;
                                pthread_mutex_unlock(&mq_lock);
                        	continue;
                        }
			pthread_mutex_unlock(&mq_lock);
			//cout << "2 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			pthread_mutex_lock (&m_lock); pthread_mutex_lock (&r_lock);
			int32_t num_tasks = rqueue.size()/2;
			if(num_tasks < 1) {
				pthread_mutex_unlock (&r_lock); pthread_mutex_unlock (&m_lock);
				continue;
			}
			try {	//cout << "going to send " << num_tasks << " tasks" << endl;
				mqueue.assign(rqueue.end()-num_tasks, rqueue.end());
				rqueue.erase(rqueue.end()-num_tasks, rqueue.end());
				//cout << "rqueue size = " << rqueue.size() << " mqueue size = " << mqueue.size() << endl;
			}
			catch (...) {
				cout << "migrateTasks: cannot allocate memory while copying tasks to migrate queue" << endl;
				pthread_exit(NULL);
			}
			pthread_mutex_unlock (&r_lock);

			map<uint32_t, NodeList> update_map = worker->get_map(mqueue);
			int update_ret = worker->zht_update(update_map, "nodehistory", index);
			/*if(index == worker->selfIndex) {
				cout << "ALERT: MIGRATING TO ITSELF" << endl;
			}*/
			int num_packages = 0;
			long total_submitted = 0;

			num_tasks = mqueue.size();
			while (total_submitted != num_tasks) {
				Package package; string alltasks;
				package.set_virtualpath(worker->ip);
				package.set_operation(22);
				num_packages++;
				int num_tasks_this_package = max_tasks_per_package;
				int num_tasks_left = num_tasks - total_submitted;
				if (num_tasks_left < max_tasks_per_package) {
                	 	       num_tasks_this_package = num_tasks_left;
				}
				for(int j = 0; j < num_tasks_this_package; j++) {
		                //TaskQueue_item* qi = migrate_queue->remove_element();
                			if(mqueue.size() < 1) {
		                	        if(j > 0) {
                			        	total_submitted = total_submitted + j;
							package.set_realfullpath(alltasks);
			                	        string str = package.SerializeAsString();
							pthread_mutex_lock(&msg_lock);
        	        		        	int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index);
							pthread_mutex_unlock(&msg_lock);
	        	                	}
        	        	        	//pthread_mutex_unlock (&m_lock);
	                	        	//return total_submitted;
		                        	//pthread_exit(NULL);
						total_submitted = num_tasks;
						break;
        	            		}
                			try {
						alltasks.append(mqueue.front()->task_id); 	alltasks.append("\'"); // Task ID
						alltasks.append(mqueue.front()->task_desc);	alltasks.append("\'");   // Task Description
						stringstream num_slots_ss;
			                        num_slots_ss << (mqueue.front()->num_slots);
						alltasks.append(num_slots_ss.str());		alltasks.append("\'\""); //Number of compute slots

                			        if(LOGGING) {
				            		migrate_fp << " taskid = " << mqueue.front()->task_id;
							migrate_fp << " task_desc = " << mqueue.front()->task_desc;
			                    		migrate_fp << " num slots = " << (mqueue.front()->num_slots);
                    		   		}
						delete mqueue.front();
		                   		mqueue.pop_front();
                	    		}
                    	    		catch (...) {
		                    		cout << "migrateTasks: Exception occurred while processing mqueue" << endl;
                	    		}
				}
				if(total_submitted == num_tasks) {
					break;
				}
				total_submitted = total_submitted + num_tasks_this_package;
				package.set_realfullpath(alltasks);
				string str = package.SerializeAsString(); //cout << "r1: " << total_submitted << " tasks" << endl;
				pthread_mutex_lock(&msg_lock);
				int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index); //cout << "r1 sent" << endl;
				pthread_mutex_unlock(&msg_lock);
			}
			pthread_mutex_unlock (&m_lock);
			//cout << "matrix_server: No. of tasks sent = " << total_submitted << endl;
		}
	}
}

void backoff() {
	usleep(waitInterval);
	if(waitInterval < waitThreshold) { // increase poll interval only if it is under 1 sec
		waitInterval *= 2;
	}
}

void resetwaitInterval() {
	waitInterval = INITIAL_WAIT_TIME;
}

/*
 * Get compute slots information from each server
 */
int32_t Worker::getComputeNodesInfo(ComputeNodesInfo &availSlots, string &task_id) {
	Package stealPackage;
        stealPackage.set_virtualpath(task_id);
        stealPackage.set_operation(14);
        string stealInfoStr = stealPackage.SerializeAsString();

	ServerSlots &serverslots = availSlots[task_id];
	int32_t totalSlots = 0;
	for(ServerSlots::iterator it = serverslots.begin(); it != serverslots.end(); ++it) {
		uint32_t serverIndex = it->first;
		if(serverIndex == selfIndex)
			continue;
		pthread_mutex_lock(&msg_lock);
		it->second = svrclient.svrtosvr(stealInfoStr, stealInfoStr.length(), serverIndex);
		pthread_mutex_unlock(&msg_lock);
		totalSlots += it->second;
	}
	return totalSlots;
}

/*
 * Randomly choose neighbors dynamically
 */
void Worker::chooseNeighbours(ComputeNodesInfo &availSlots, string &task_id) {
	srand(time(NULL));
	int i, ran;
	int x;
	char *flag = new char[num_nodes];
	memset(flag, '0', num_nodes);

	ServerSlots serverslots;	

	for(i = 0; i < num_neigh; i++) {
		ran = rand() % num_nodes;		
		x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		while(flag[ran] == '1' || !x) {
			ran = rand() % num_nodes;
			x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		}
		flag[ran] = '1';
		serverslots[ran] = 0;
	}
	availSlots[task_id] = serverslots;
	delete flag;
}

/*
 * Steal compute slots from the neighbors
 */
int32_t Worker::stealSlots(ComputeNodesInfo &availSlots, string &task_id) {
	chooseNeighbours(availSlots, task_id);
	int32_t totalSlots = getComputeNodesInfo(availSlots, task_id);
	while(totalSlots <= 0) {
		backoff();
		chooseNeighbours(availSlots, task_id);
		totalSlots = getComputeNodesInfo(availSlots, task_id);
	}
	resetwaitInterval();
	return totalSlots;
}

int Worker::checkIfTaskIsReady(string key) {
	int index = myhash(key.c_str(), svrclient.memberList.size());
	if(index != selfIndex) {
		Package check_package;
		check_package.set_virtualpath(key);
		check_package.set_operation(23);
		string check_str = check_package.SerializeAsString();
		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.svrtosvr(check_str, check_str.size(), index);
		pthread_mutex_unlock(&msg_lock);
		return ret;
	}
	else {
		string value = zht_lookup(key);
		Package check_package;
		check_package.ParseFromString(value);
		return check_package.numwait();
	}
}

int Worker::moveTaskToReadyQueue(TaskQueue_Item **qi) {
	pthread_mutex_lock(&r_lock);
	rqueue.push_back(*qi);
	pthread_mutex_unlock(&r_lock);
}

bool eraseCondition(TaskQueue_Item *qi) {
	return qi==NULL;
}

void* checkWaitQueue(void* args) {
	Worker *worker = (Worker*)args;
	TaskQueue_Item *qi;
        while(ON) {
                while(wqueue.size() > 0) {
			int size = wqueue.size();
			for(int i = 0; i < size; i++) {
				qi = wqueue[i];
				if(qi != NULL) {
					int status = worker->checkIfTaskIsReady(qi->task_id);
					if(status == 0) {
						int ret = worker->moveTaskToReadyQueue(&qi);
						pthread_mutex_lock(&w_lock);
						wqueue[i] = NULL;
						pthread_mutex_unlock(&w_lock);
					}
				}
			}
			pthread_mutex_lock(&w_lock);
			TaskQueue::iterator last = remove_if(wqueue.begin(), wqueue.end(), eraseCondition);
			wqueue.erase(last, wqueue.end());
			pthread_mutex_unlock(&w_lock);
			sleep(1);
		}
	}
}

uint32_t Worker::freeLockedSlots(string &task_id) {
	SlotList &slotList = lockedNodes[task_id];
	ComputeNodesStatus::iterator status_it = nodeStatus.begin();
	pthread_mutex_lock(&mutex_idle);
	for(SlotList::iterator list_it = slotList.begin(); list_it != slotList.end(); ++list_it) {
		nodeStatus[*list_it] = true;
		num_idle_cores++;
	}
	slotList.clear();
	pthread_mutex_unlock(&mutex_idle);
	return 0;
}

int32_t Worker::freeLockedSlots(ComputeNodesInfo &availSlots, string &task_id) {
	freeLockedSlots(task_id);
	ServerSlots &serverslots = availSlots[task_id];
	serverslots.erase(selfIndex);
	if(serverslots.size() < 1)
		return 0;

	Package freePackage;
	freePackage.set_virtualpath(task_id);
	freePackage.set_operation(13);
	string freeSlotsStr(freePackage.SerializeAsString());
	for(ServerSlots::iterator it = serverslots.begin(); it != serverslots.end(); ++it) {
                uint32_t serverIndex = it->first;
                if(serverIndex == selfIndex)
                        continue;
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.svrtosvr(freeSlotsStr, freeSlotsStr.length(), serverIndex);
                pthread_mutex_unlock(&msg_lock);
        }
	availSlots.erase(task_id);
        return 0;	
}

uint32_t Worker::getHalfSlots(string &task_id) {
	pthread_mutex_lock(&mutex_idle);
	uint32_t slots_locked = num_idle_cores/2;
	if(slots_locked < 1) {
		pthread_mutex_unlock(&mutex_idle);
		return 0;
	}
	num_idle_cores -= slots_locked;
	ComputeNodesStatus::iterator status_it = nodeStatus.begin();
	int i = 0;
	SlotList slotList;
	while(status_it != nodeStatus.end()) {
		if(status_it->second) {
			status_it->second = false;
			slotList.push_back(status_it->first);
			i++;
		}
		if(i == slots_locked) {
			break;
		}
		++status_it;
	}
	pthread_mutex_unlock(&mutex_idle);
	lockedNodes[task_id] = slotList;
	return slots_locked;
}

ComputeNodesInfo Worker::getAvailSlotsInfo(uint32_t &numAvailSlots, string &task_id, uint32_t &reqdSlots) {
	ComputeNodesInfo availSlots;
	uint32_t slotsLocked = getHalfSlots(task_id);
	numAvailSlots = slotsLocked;
	if (num_nodes <= 1 || reqdSlots <= slotsLocked) {
		ServerSlots &serverslots = availSlots[task_id];
		serverslots[selfIndex] = slotsLocked;
		return availSlots;
	}
	numAvailSlots += stealSlots(availSlots, task_id);
	ServerSlots &serverslots = availSlots[task_id];
        serverslots[selfIndex] = slotsLocked;
	return availSlots;
}

bool Worker::checkIfReqdSlotsAvail(TaskQueue_Item *qi, uint32_t &numAvailSlots){
	return (qi->num_slots<=numAvailSlots);
}

void* checkReadyQueue(void* args) {

	Worker *worker = (Worker*)args;
	TaskQueue_Item *qi;
        while(ON) {
                while(rqueue.size() > 0) {
                        uint32_t numAvailSlots = 0;
                        ComputeNodesInfo availSlots;
			int exec_flag = 0;

			int size = rqueue.size();
			for(int i = 0; i < size; i++) {
				qi = rqueue[i];
				if(qi != NULL) {
					availSlots = worker->getAvailSlotsInfo(numAvailSlots, qi->task_id, qi->num_slots);
					bool ready = worker->checkIfReqdSlotsAvail(qi, numAvailSlots);
					if(ready) {
						int ret = worker->executeTask(qi, availSlots[qi->task_id]);
						pthread_mutex_lock(&r_lock);
						rqueue[i] = NULL;
						pthread_mutex_unlock(&r_lock);
						exec_flag = 1;
						//break;
					}
					else {
                		                int ret = worker->freeLockedSlots(availSlots, qi->task_id);
                        		}
				}
			}
			if(!exec_flag) {
				backoff();
			}
			pthread_mutex_lock(&r_lock);
			TaskQueue::iterator last = remove_if(rqueue.begin(), rqueue.end(), eraseCondition);
			rqueue.erase(last, rqueue.end());
			pthread_mutex_unlock(&r_lock);
			//sleep(1);
		}
	}
}

int32_t Worker::executeTask(TaskQueue_Item *qi, ServerSlots &serverslots) {
	Package execPackage;
        execPackage.set_virtualpath(task_id);
        execPackage.set_operation(12);
        string execlotsStr(execPackage.SerializeAsString());
	for(ServerSlots::iterator it = serverslots.begin(); it != serverslots.end(); ++it) {
                uint32_t serverIndex = it->first;
                if(serverIndex == selfIndex) {
			
                        continue;
		}
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.svrtosvr(execSlotsStr, execSlotsStr.length(), serverIndex);
                pthread_mutex_unlock(&msg_lock);
        }
}

static int work_exec_flag = 0;
// thread to monitor ready queue and execute tasks based on num of cores availability
void* executeThread(void* args) {

	Worker *worker = (Worker*)args;

	TaskQueue_Item *qi;
	while(ON) {
		while(rqueue.size() > 0) {
				pthread_mutex_lock(&r_lock);
					if(rqueue.size() > 0) {						
						qi = rqueue.front();
						rqueue.pop_front();
					}
					else {
						pthread_mutex_unlock(&r_lock);
						continue;
					}
				
                                pthread_mutex_unlock(&r_lock);

                                pthread_mutex_lock(&mutex_idle);
                                worker->num_idle_cores--;
                                pthread_mutex_unlock(&mutex_idle);
						
				if(!work_exec_flag) {
					work_exec_flag = 1;
					worker_start << worker->ip << ":" << worker->selfIndex << " Got jobs..Started excuting" << endl;
				}

				long duration;
				stringstream duration_ss;
				duration_ss << qi->task_desc;
				duration_ss >> duration;

				//cout << "task to lookup = " << qi->task_id << endl;
				string value = worker->zht_lookup(qi->task_id);
				Package recv_pkg;
			        recv_pkg.ParseFromString(value); //cout << "check_ready_queue: task " << qi->task_id << " node history = " << recv_pkg.nodehistory() << endl;
				int num_vector_count, per_vector_count;
	                        vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
				
				try {
					duration_ss <<  tokenize_string.at(0).at(1);
				}
				catch (exception& e) {
					cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(1)) " << " " << e.what() << endl;
					cout << "void* check_ready_queue: value = " << value << endl;
                                        exit(1);
                                }
				//duration = 1000000;
				duration_ss >> duration;

				string client_id;
				try {
					client_id = tokenize_string.at(0).at(2);
				}
				catch (exception& e) {
                                        cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(2)) " << " " << e.what() << endl;
                                        exit(1);
                                }
                 
				uint64_t sub_time;
				try {
					stringstream sub_time_ss;
					sub_time_ss << tokenize_string.at(0).at(3);
					sub_time_ss >> sub_time;
				}
				catch (exception& e) {
                                        cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(3)) " << " " << e.what() << endl;
                                        exit(1);
                                }
				
				timespec task_start_time, task_end_time;
				clock_gettime(CLOCK_REALTIME, &task_start_time);
				//uint32_t exit_code = sleep(duration);
				uint32_t exit_code = usleep(duration);
				clock_gettime(CLOCK_REALTIME, &task_end_time);
				
				// push completed task into complete queue
				pthread_mutex_lock(&c_lock);
				cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(1)));
				pthread_mutex_unlock(&c_lock);
						
				// append completed task
				uint64_t st = (uint64_t)task_start_time.tv_sec * 1000000000 + (uint64_t)task_start_time.tv_nsec;
				uint64_t et = (uint64_t)task_end_time.tv_sec * 1000000000 + (uint64_t)task_end_time.tv_nsec;
				timespec diff = timediff(task_start_time, task_end_time);
						
				pthread_mutex_lock(&mutex_idle);
				worker->num_idle_cores++; task_comp_count++;
				pthread_mutex_unlock(&mutex_idle);
						
				if(LOGGING) {
					string fin_str;
					stringstream out;
					out << qi->task_id << " exitcode " << exit_code << " Interval " << diff.tv_sec << " S  " << diff.tv_nsec << " NS" << " server " << worker->ip;
					fin_str = out.str();
					pthread_mutex_lock(&mutex_finish);
					fin_fp << fin_str << endl;
					pthread_mutex_unlock(&mutex_finish);
				}						
				delete qi;
		}
	}
}

int Worker::notify(ComPair &compair) {
	map<uint32_t, NodeList> update_map = worker->get_map(compair.second);
	int update_ret = worker->zht_update(update_map, "numwait", selfIndex);
	nots += compair.second.size(); log_fp << "nots = " << nots << endl;
	return update_ret;
	/*cout << "task = " << compair.first << " notify list: ";
	for(int l = 0; l < compair.second.size(); l++) {
		cout << compair.second.at(l) << " ";
	} cout << endl;*/
}

void* checkCompleteQueue(void* args) {
	Worker *worker = (Worker*)args;

        ComPair compair;
        while(ON) {
                while(cqueue.size() > 0) {
                                pthread_mutex_lock(&c_lock);
                                        if(cqueue.size() > 0) {
                                                compair = cqueue.front();
                                                cqueue.pop_front();
                                        }
                                        else {
                                                pthread_mutex_unlock(&c_lock);
                                                continue;
                                        }
                                pthread_mutex_unlock(&c_lock);
				worker->notify(compair);
		}
	}
}

int32_t Worker::get_load_info() {
	return (rqueue.size() - num_idle_cores);
}

int32_t Worker::get_compute_slots_info() {
	return num_idle_cores;
}

int32_t Worker::get_monitoring_info() {
	if (LOGGING) {
		log_fp << "rqueue = " << rqueue.size() << " mqueue = " << mqueue.size() << " wqueue = " << wqueue.size() << endl;
	}
	return (((rqueue.size() + mqueue.size() + wqueue.size()) * 10) + num_idle_cores);
}

int32_t Worker::get_numtasks_to_steal() {
	return ((rqueue.size() - num_idle_cores)/2);
}

string Worker::zht_lookup(string key) {
	string task_str;

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(1);
	string lookup_str = package.SerializeAsString();
	int index;
	svrclient.str2Host(lookup_str, index);*/

	int index = myhash(key.c_str(), svrclient.memberList.size());

	if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(1);
	        string lookup_str = package.SerializeAsString();

		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.lookup(lookup_str, task_str);
		pthread_mutex_unlock(&msg_lock);

		Package task_pkg;
        	task_pkg.ParseFromString(task_str);
		//cout << "remote lookup: string = " << task_str << " str size = " << task_str.size() << endl;
	        //cout << "remote lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	else {
		string *result = pmap->get(key);
		if (result == NULL) {
			cout << "lookup find nothing. key = " << key << endl;
			string nullString = "Empty";
			return nullString;
		}
		task_str = *result;
		//Package task_pkg;
        	//task_pkg.ParseFromString(task_str);
	        //cout << "local lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	return task_str;
	/*Package task_pkg;
	task_pkg.ParseFromString(task_str);
	return task_pkg.realfullpath();*/
}

int Worker::zht_insert(string str) {
	Package package;
	package.ParseFromString(str);
        package.set_operation(3);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	if(index != selfIndex) {
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.insert(str);
                pthread_mutex_unlock(&msg_lock);
		return ret;
        }
	else {
		string key = package.virtualpath();
		//pthread_mutex_lock(&zht_lock);
		int ret = pmap->put(key, str);
		//pthread_mutex_unlock(&zht_lock);
		if (ret != 0) {
			cerr << "insert error: key = " << key << " ret = " << ret << endl;
			return -3;
		}
		else
			return 0;
	}
}

int Worker::zht_remove(string key) {

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(2);
	string str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);*/

	int index = myhash(key.c_str(), svrclient.memberList.size());

        if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(2);
	        string str = package.SerializeAsString();
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.remove(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
                int ret = pmap->remove(key);
		if (ret != 0) {
			cerr << "DB Error: fail to remove :ret= " << ret << endl;
			return -2;
		} else
			return 0; //succeed.
        }

}

int Worker::zht_append(string str) {
	Package package;
	package.ParseFromString(str);
	package.set_operation(4);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	if(index != selfIndex) {
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.append(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
		string key = package.virtualpath();
		int ret = pmap->append(key, str);
		if (ret != 0) {
			cerr << "Append error: ret = " << ret << endl;
			return -4;
		} else
			return 0;
	}
}

int Worker::update_nodehistory(uint32_t currnode, string alltasks) {
	int num_vector_count, per_vector_count;
	vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	//cout << "Worker = " << selfIndex << " num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                	string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);

			int index = myhash(taskid.c_str(), num_nodes);
                        if(index != selfIndex) {
                                cout << "something wrong..doing remote update_nodehistory index = " << index << " selfIndex = " << selfIndex << endl;
                        }

			// update number of moves (increment)
			uint32_t old_nummoves = recv_pkg.nummoves();
			recv_pkg.set_nummoves(old_nummoves+1);

			// update current location of task
			recv_pkg.set_currnode(currnode);

			// update task migration history
			stringstream nodehistory_ss;
			nodehistory_ss << currnode << "\'";
			string new_nodehistory(nodehistory_ss.str());
			new_nodehistory.append(recv_pkg.nodehistory()); //cout << "update_nodehistory: task " << recv_pkg.virtualpath() << " node history = " << recv_pkg.nodehistory();
			recv_pkg.set_nodehistory(new_nodehistory); //cout << " node history = " << recv_pkg.nodehistory() << endl;

			// insert updated task into ZHT
			int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_nodehistory: zht_insert error ret = " << ret << endl;
				exit(1);
			}
                }
                catch (exception& e) {
                	cout << "update_nodehistory: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
	}
	return 0;
}

int Worker::update_numwait(string alltasks) {
	int num_vector_count, per_vector_count;
        vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                        string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);
	
			int index = myhash(taskid.c_str(), num_nodes);
			if(index != selfIndex) {
				cout << "something wrong..doing remote update_numwait: index = " << index << " selfIndex = " << selfIndex << endl;
			}

			// update number of tasks to wait (decrement)
			uint32_t old_numwait = recv_pkg.numwait();
                        recv_pkg.set_numwait(old_numwait-1);
			notr++;
			if(LOGGING) {
				if(old_numwait-1 == 0) {
					log_fp << "task = " << taskid << " is ready" << endl;
				}
			}

			// insert updated task into ZHT
                        int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_numwait: old_numwait = " << old_numwait << endl;
                                cout << "update_numwait: zht_insert error ret = " << ret << " key = " << taskid << " index = " << index << " selfindex = " << selfIndex << endl;
                                exit(1);
                        }
                }
                catch (exception& e) {
                        cout << "update_numwait: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
        }
	log_fp << "notr = " << notr << endl;
	return 0;
}

int Worker::update(Package &package) {
	string field(package.virtualpath());
	if(field.compare("nodehistory") == 0) {
		return update_nodehistory(package.currnode(), package.realfullpath());
	}
	else if(field.compare("numwait") == 0) {
		return update_numwait(package.realfullpath());
	}
}

int Worker::zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid = 0) {

	Package package;
        package.set_operation(25);
	package.set_virtualpath(field);
        if(!field.compare("nodehistory")) {
                package.set_currnode(toid);
        } //cout << "deque size = ";
	for(map<uint32_t, NodeList>::iterator map_it = update_map.begin(); map_it != update_map.end(); ++map_it) {
		uint32_t index = map_it->first;
		NodeList &update_list = map_it->second;
		//cout << update_list.size() << " ";
		while(!update_list.empty()) {
			package.set_realfullpath(update_list.front());
			update_list.pop_front();
			if(index == selfIndex) {
				string *str;
                        	str = new string(package.SerializeAsString());
                        	pthread_mutex_lock(&notq_lock);
                        	notifyq.push(str);
                        	pthread_mutex_unlock(&notq_lock);
				//int ret = update(package);
			}
			//else if (index != toid){
			else {
				string update_str = package.SerializeAsString();
				pthread_mutex_lock(&msg_lock);
				int ret = svrclient.svrtosvr(update_str, update_str.size(), index);
				pthread_mutex_unlock(&msg_lock);
			}
		}
	} //cout << endl;
}


