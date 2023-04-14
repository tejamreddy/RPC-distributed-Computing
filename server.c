#include <rpc/rpc.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <signal.h>

#include "replicator.h"

static Status server_status = Idle;

static pid_t child_pid;
static char * cur_job;

Status * status_1_svc(void * arg, struct svc_req * req){
	static Status res;
	res = server_status;
	return &res;
}

float * cpuload_1_svc(void * arg, struct svc_req * req){
	static float res;
	FILE * f = fopen("/proc/loadavg", "r");

	if(f){
		fscanf(f, "%f", &res);
		fclose(f);
	}

	return &res;
}

static void on_job_finished(int sig){
	int child_status;
	wait(&child_status);

	if(WEXITSTATUS(child_status)){
		fprintf(stderr, "command execution failed!\n");
		server_status = Failed;
	}else{
		if(server_status != Stopped){
			server_status = Finished;
		}
	}
}

static void reap_child(int sig){
	(void)sig;

	if(server_status == Running){
		killpg(getpgid(child_pid), SIGKILL);
	}

	exit(1);
}

int * startjob_1_svc(char ** job, struct svc_req * req){
	signal(SIGCHLD, on_job_finished);
	signal(SIGINT, reap_child);
	signal(SIGTERM, reap_child);
	signal(SIGKILL, reap_child);
	signal(SIGTSTP, reap_child);

	static int res;

	if(server_status == Running){
		res = 2;
		return &res;
	}

	child_pid = fork();

	if(child_pid == -1){
		perror("fork");
		res = 1;
		return &res;
	}

	if(!child_pid){
		setpgid(0, 0);
		execl("/bin/bash", "bash", "-c", *job, (void *)0);
		perror("execl");
		exit(1);
	}else{
		server_status = Running;

		if(*job != cur_job){
			const int job_len = strlen(*job) + 1;
			cur_job = malloc(job_len);
			strcpy(cur_job, *job);
			cur_job[job_len - 1] = '\0';
		}

		res = 0;
	}

	return &res;
}

int * stop_1_svc(void * arg, struct svc_req * req){
	static int res;

	switch(server_status){
		case Running :
			killpg(getpgid(child_pid), SIGKILL);
			server_status = Stopped;
			res = 0;
			break;

		case Stopped :
			res = 1;
			break;

		case Idle : 
			res = 2;
			break;

		default :
			res = 3;
	}

	return &res;
}

int * restart_1_svc(void * arg, struct svc_req * req){
	static int res;
	res = *stop_1_svc(arg, req);

	if(!res){
		startjob_1_svc(&cur_job, req);
	}else if(res == 1){
		server_status = Idle;
	}

	return &res;
}
