#include <stdio.h>
#include <string.h>
#include <rpc/rpc.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include "replicator.h"


typedef struct {
	char ** hosts;
	int num_servers;
} Servers;

typedef struct {
	int num;
	int num_batches;
	int batch_size;
	int lambda;
	int mu;
	int c2;
	int stat_batch_size;
	char * assigned_to;
	Status status;
} Job;

typedef struct {
	Job * jobs;
	int num_jobs;
} Jobs;

#define CLIENT_TIMEOUT 3

Servers servers;
Jobs pending_jobs = {NULL, 0};
float low_threshold = 1.5;
float high_threshold = 3;

void set_client_timeout(CLIENT * client, const int timeout_secs){
	struct timeval tv;
	tv.tv_sec = timeout_secs;
	tv.tv_usec = 0;
	clnt_control(client, CLSET_TIMEOUT, (char*)&tv);
}

int give_job(CLIENT * client, const Job * const job){
	char command[1024] = {"./hyper_link "};
	char num_buffer[32];

	const int cmd_parameters[7] = {job->num, job->num_batches, job->batch_size, job->lambda, job->mu, job->c2, job->stat_batch_size};

	for(size_t i = 0; i < sizeof(cmd_parameters) / sizeof(int); ++i){
		sprintf(num_buffer, "%d", cmd_parameters[i]);
		strcat(command, num_buffer);
		strcat(command, " ");
	}

	const char * home_path = getenv("HOME");

	if(!home_path){
		return -1;
	}

	char path[256];
	strcpy(path, home_path);
	strcat(path, "/replicate_out/");

	char job_num[32];
	sprintf(job_num, "%d", job->num);
	strcat(path, "job-");
	strcat(path, job_num);
	strcat(command, " | tee ");
	strcat(command, path);

	char * cmd_ptr = &command[0];
	const int * res = startjob_1(&cmd_ptr, client);
	return res ? *res : 1;
}

void truncate_jobs(const char * hostname){
	int write_idx = 0;

	for (int i = 0; i < pending_jobs.num_jobs; ++i) {

		if(!pending_jobs.jobs[i].assigned_to || strcmp(pending_jobs.jobs[i].assigned_to, hostname)){
			pending_jobs.jobs[write_idx++] = pending_jobs.jobs[i];
		}
	}

	pending_jobs.num_jobs = write_idx;

	if(pending_jobs.num_jobs){
		pending_jobs.jobs = realloc(pending_jobs.jobs, sizeof(Job) * pending_jobs.num_jobs);
	}else{
		pending_jobs.jobs = NULL;
	}
}

void remove_job_assignment(const char * hostname){

	for (int i = 0; i < pending_jobs.num_jobs; ++i) {

		if(pending_jobs.jobs[i].assigned_to && !strcmp(pending_jobs.jobs[i].assigned_to, hostname)){
			pending_jobs.jobs[i].assigned_to = NULL;
			pending_jobs.jobs[i].status = Idle;
			return;
		}
	}
}

void jobs_loop(int sig){
	(void)sig;

	for(int i = 0; i < servers.num_servers; ++i){
		CLIENT * client = clnt_create(servers.hosts[i], REPLICATORPROG, REPLICATORVERS, "tcp");

		if(!client){
			remove_job_assignment(servers.hosts[i]);
			continue;
		}

		set_client_timeout(client, CLIENT_TIMEOUT);

		const float * server_load = cpuload_1(NULL, client);
		const Status * server_status = status_1(NULL, client);

		if(!server_load || !server_status){
			remove_job_assignment(servers.hosts[i]);
			continue;
		}

		if(*server_status == Finished){
			truncate_jobs(servers.hosts[i]);
		}else if(*server_status == Failed || *server_status == Stopped){
			remove_job_assignment(servers.hosts[i]);
		}

		if(*server_load <= low_threshold && (*server_status != Running && *server_status != Stopped)){

			for(int j = 0; j < pending_jobs.num_jobs; ++j){

				if(!pending_jobs.jobs[j].assigned_to){
					pending_jobs.jobs[j].assigned_to = servers.hosts[i];
					give_job(client, &pending_jobs.jobs[j]);
					break;
				}
			}

		}else if(*server_load > high_threshold && *server_status == Running){
			printf("stopping %s because of high cpu load (%f/%f)!\n", servers.hosts[i], *server_load, (float)high_threshold);
			stop_1(NULL, client);
		}

		clnt_destroy(client);
	}

	alarm(3);
}

Servers obtain_servers(const char * const hosts_path){
	Servers servers;
	memset(&servers, 0, sizeof(servers));

	FILE * const f = fopen(hosts_path, "r");

	if(!f){
		fprintf(stderr, "error opening replicate.hosts. exiting...\n");
		return servers;
	}

	size_t line_size = 0;

	for(char * server; server = NULL, getline(&server, &line_size, f) > 0;){
		const int server_len = strlen(server);

		if(server_len == 1){
			continue;
		}

		servers.hosts = realloc(servers.hosts, (servers.num_servers + 1) * sizeof(char*));
		server[strcspn(server, "\n")] = '\0';
		servers.hosts[servers.num_servers++] = server;
	}

	fclose(f);
	return servers;
}

void show_servers(void){
	printf("registered servers: \n");

	for(int i = 0; i < servers.num_servers; ++i){
		printf("%d - %s\n", i + 1, servers.hosts[i]);
	}
}

int create_directory(const char *path) {
	struct stat st;

	if(stat(path, &st) == 0 && S_ISDIR(st.st_mode)){
		return 0;
	}

	if(mkdir(path, 0777) == -1){
		return errno == EEXIST ? 0 : -1;
	}

	return 0;
}

typedef struct {
	CLIENT * clnt;
	const char * hostname;
} Client;

Client get_server_client(const char * input){
	Client client = {NULL, NULL};

	if(!input){
		fprintf(stderr, "command missing server number!\n");
		return client;
	}

	int server_num;

	if(!sscanf(input, "%d", &server_num)){
		return client;
	}

	--server_num;

	if(server_num < 0 || server_num >= servers.num_servers){
		fprintf(stderr, "invalid server number!\n");
		return client;
	}

	client.hostname = servers.hosts[server_num];
	client.clnt = clnt_create(servers.hosts[server_num], REPLICATORPROG, REPLICATORVERS, "tcp");

	if(!client.clnt){
		fprintf(stderr, "%s not reachable! (inactive)\n", servers.hosts[server_num]);
	}else{
		set_client_timeout(client.clnt, CLIENT_TIMEOUT);
	}

	return client;
}


void stop_all_servers(void){
	for(int i = 0; i < servers.num_servers; ++i){
		CLIENT * client = clnt_create(servers.hosts[i], REPLICATORPROG, REPLICATORVERS, "tcp");

		if(!client){
			continue;
		}

		set_client_timeout(client, CLIENT_TIMEOUT);
		stop_1(NULL, client);
	}
}

int check_result(const void * result){

	if(!result){
		fprintf(stderr, "connection reset by server!\n");
		return 1;
	}

	return 0;
}

int main(void){

	{
		const char * home_path = getenv("HOME");

		if(!home_path){
			fprintf(stderr, "$HOME not set. please set it and restart client.\n");
			return 1;
		}

		char output_path[512];

		strcpy(output_path, home_path);
		strcat(output_path, "/replicate_out");
		strcat(output_path, "\0");

		if(create_directory(output_path)){
			fprintf(stderr, "could not create output directory. exiting!\n");
			return 1;
		}

		char hosts_path[512];
		strcpy(hosts_path, home_path);
		strcat(hosts_path, "/replicate.hosts");
		strcat(hosts_path, "\0");

		servers = obtain_servers(hosts_path);

		if(!servers.num_servers){
			fprintf(stderr, "no servers found in replicate.hosts. exiting...\n");
			return 1;
		}
	}

	printf("----\n");
	show_servers();
	printf("----\n");

	signal(SIGALRM, jobs_loop);
	alarm(5);

	size_t line_len = 0;

	for(char * cmd; printf("> "), getline(&cmd, &line_len, stdin) > 1;){
		cmd[strcspn(cmd, "\n")] = '\0';

		if(!strcmp(cmd, "help")){
			printf("available commands: ([n] represents server number)\n- list : list registered servers\n"
			       "- hyper_link : run command on registered servers\n"
			       "- jobs : prints information about remaining jobs\n"
			       "- setlow : set low cpu load threshold for all servers (default: 1.5)\n"
			       "- sethigh : set high cpu load threshold for all servers (default: 3.0)\n"
			       "- status [n] : show status of server\n"
			       "- load [n] : show load of server\n"
			       "- stop [n] : stop current server\n- restart [n] : restart server\n");
			continue;
		}

		if(!strcmp(cmd, "jobs")){

			if(!pending_jobs.num_jobs){
				printf("no jobs registered yet!\n");
				continue;
			}

			for(int i = 0; i < pending_jobs.num_jobs; ++i){
				printf("Job # %d -> ", pending_jobs.jobs[i].num);

				if(pending_jobs.jobs[i].assigned_to){
					printf("assigned to %s. status: ", pending_jobs.jobs[i].assigned_to);

					const Status status = pending_jobs.jobs[i].status;

					if(status == Running){
						printf("running\n");
					}else if(status == Idle){
						printf("pending\n");
					}else if(status == Finished){
						printf("finished\n");
					}
				}else{
					printf("unassigned\n");
				}
			}

			continue;
		}

		if(!strcmp(cmd, "list")){
			show_servers();
			continue;
		}

		const char * token = strtok(cmd, " ");

		if(!token){
			fprintf(stderr, "invalid command\n");
			continue;
		}

		if(!strcmp(token, "setlow")){
			token = strtok(NULL, " ");

			if(!token){
				fprintf(stderr, "missing load value!\n");
				continue;
			}

			float load;

			if(sscanf(token, "%f", &load) == 1){

				if(low_threshold > high_threshold){
					fprintf(stderr, "low cpu load threshold cannot be greater than high threshold!\n");
					continue;
				}

				low_threshold = load;
				printf("changed cpu low load threshold to %f sucessfully!\n", load);
			}else{
				fprintf(stderr, "could not extract load value!\n");
			}

			continue;
		}

		if(!strcmp(token, "sethigh")){
			token = strtok(NULL, " ");

			if(!token){
				fprintf(stderr, "missing load value!\n");
				continue;
			}

			float load;

			if(sscanf(token, "%f", &load) == 1){

				if(high_threshold < low_threshold){
					fprintf(stderr, "high cpu load threshold cannot be smaller than low threshold!\n");
					continue;
				}

				high_threshold = load;
				printf("changed cpu high load threshold to %f sucessfully!\n", load);
			}else{
				fprintf(stderr, "could not extract load value!\n");
			}

			continue;
		}

		if(!strcmp(token, "stop")){
			Client client = get_server_client(strtok(NULL, " "));

			if(!client.clnt){
				continue;
			}

			const int * res = stop_1(NULL, client.clnt);

			if(check_result(res)){
				continue;
			}

			if(!*res){
				printf("%s stopped successfully!\n", client.hostname);
			}else{
				printf("%s is already idle!\n", client.hostname);
			}

			clnt_destroy(client.clnt);
			continue;
		}

		if(!strcmp(cmd, "status")){
			Client client = get_server_client(strtok(NULL, " "));

			if(!client.clnt){
				continue;
			}

			const Status * status = status_1(NULL, client.clnt);

			if(check_result(status)){
				continue;
			}

			if(*status == Running){
				printf("%s is running a command! (active)\n", client.hostname);
			}else if(*status == Idle || *status == Finished || *status == Failed){
				printf("%s is idle! (active)\n", client.hostname);
			}else if(*status == Stopped){
				printf("%s is stopped! (active and idle)\n", client.hostname);
			}else{
				// should never happen
				printf("%s is ??\n", client.hostname);
			}

			clnt_destroy(client.clnt);
			continue;
		}

		if(!strcmp(cmd, "restart")){
			Client client = get_server_client(strtok(NULL, " "));

			if(!client.clnt){
				continue;
			}

			const int * res = restart_1(NULL, client.clnt);

			if(check_result(res)){
				continue;
			}

			if(*res <= 2){
				printf("%s is running!\n", client.hostname);
			}else{
				printf("%s is either in finished or failed state. client will restart the server soon!\n", client.hostname);
			}

			clnt_destroy(client.clnt);
			continue;
		}

		if(!strcmp(cmd, "load")){
			Client client = get_server_client(strtok(NULL, " "));

			if(!client.clnt){
				continue;
			}

			float * res = cpuload_1(NULL, client.clnt);

			if(check_result(res)){
				continue;
			}

			printf("CPU LOAD of %s: %f\n", client.hostname, *res);
			clnt_destroy(client.clnt);
			continue;
		}

		if(!strcmp(token, "hyper_link")){
			int params[9];
			memset(params, 0, sizeof(params));

			int valid_cmd = 1;

			for(size_t i = 0; i < sizeof(params) / sizeof(int); i++){
				token = strtok(NULL, " ");

				if(!token){
					fprintf(stderr, "not enough arguments for hyper_link command!\n");
					valid_cmd = 0;
					break;
				}

				params[i] = atoi(token);
			}

			if(valid_cmd){
				const int starting_job_num = params[0];
				const int ending_job_num = params[1];
				const int job_step = params[2];

				Job job;
				job.assigned_to = NULL;
				job.status = Idle;
				job.num_batches = params[3];
				job.batch_size = params[4];
				job.lambda = params[5];
				job.mu = params[6];
				job.c2 = params[7];
				job.stat_batch_size = params[8];

				for(int i = starting_job_num; i <= ending_job_num; i += job_step){
					job.num = i;
					pending_jobs.jobs = realloc(pending_jobs.jobs, sizeof(Job) * ++pending_jobs.num_jobs);
					pending_jobs.jobs[pending_jobs.num_jobs - 1] = job;
				}
			}

			continue;
		}

		fprintf(stderr, "%s", "Unrecognized command!\n");
	}

	stop_all_servers();

	for(int i = 0; i < servers.num_servers; ++i){
		free(servers.hosts[i]);
	}

	free(servers.hosts);
	free(pending_jobs.jobs);
	printf("exiting...\n");
}
