enum Status {
	Running,
	Idle,
	Finished,
	Stopped,
	Failed
};

program REPLICATORPROG {
	version REPLICATORVERS {
		float CPULOAD() = 1;
		int STARTJOB(string) = 2;
		Status STATUS() = 3;
		int STOP() = 4;
		int RESTART() = 5;
	} = 1;
} = 0x20000001;
