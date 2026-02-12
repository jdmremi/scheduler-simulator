#!/usr/bin/env python3
from argparse import ArgumentParser

class Job:
    def __init__(self, arrival_time, run_time, job_number):
        self.arrival_time              = arrival_time
        self.remaining_burst_time      = run_time
        self.total_run_time            = run_time
        self.job_number                = job_number
        self.first_run_time            = -1
        self.completion_time           = -1
        self.is_first_run              = True
        # turnaround time              = completion time - arrival time (time between job arrival and job completion)
        # response time                = first run time  - arrival time (time until a job starts producing results)
        # wait time                    = turnaround time - run time     (time spent in the ready queue waiting to run)
        self.calculate_turnaround_time = lambda: self.completion_time             - self.arrival_time
        self.calculate_response_time   = lambda: self.first_run_time              - self.arrival_time
        self.calculate_wait_time       = lambda: self.calculate_turnaround_time() - self.total_run_time
    
    # sets the is_first_run flag to False and the first run time to the specified value if first time running
    def update_first_run(self, time):
       if self.is_first_run:
            self.first_run_time = time
            self.is_first_run   = False 
    
    @staticmethod
    def read_jobs(input_file):
        with open(input_file, "r") as file:
            lines = sorted([line.split(" ") for line in file.readlines()], key=lambda token: int(token[1]))
            jobs  = [Job(run_time=int(job[0]), arrival_time=int(job[1]), job_number=i) for i, job in enumerate(lines)]
            return jobs

class Scheduler:
    def __init__(self, quantum, job_list):
        self.jobs              = []
        self.completed_jobs    = []
        self.print_str         = ""
        self.fifo_srtn_current = 0
        self.total_time        = 0
        self.quantum           = max(1, quantum)
        self.scheduler         = self.scheduler_fifo
        self.jobs.extend(job_list)

    def start_scheduler(self):
        if len(self.jobs):
            self.scheduler()

    def scheduler_fifo(self):
        while len(self.jobs) != len(self.completed_jobs):
            scheduled = self.jobs[self.fifo_srtn_current]
            if not self.__scheduled_job_has_arrived(scheduled):
                continue
            
            scheduled.update_first_run(self.total_time)
            self.__update_print_str(scheduled.job_number, scheduled.remaining_burst_time)
            self.total_time                += scheduled.remaining_burst_time
            scheduled.completion_time      = self.total_time
            scheduled.remaining_burst_time = 0
            self.completed_jobs.append(scheduled)
            self.fifo_srtn_current += 1
        
    def scheduler_rr(self):
        while len(self.jobs) != len(self.completed_jobs):
            scheduled = self.jobs[self.fifo_srtn_current]
            if not self.__scheduled_job_has_arrived(scheduled):
                continue
            
            scheduled.update_first_run(self.total_time)

            remaining_burst = scheduled.remaining_burst_time
            # if remaining burst in (0, self.quantum], run to completion. otherwise, run a slice.
            if remaining_burst <= self.quantum and remaining_burst > 0:
                self.__update_print_str(scheduled.job_number, remaining_burst)
                self.total_time                += remaining_burst
                scheduled.completion_time      = self.total_time
                scheduled.remaining_burst_time = 0
                self.completed_jobs.append(scheduled)
            elif remaining_burst > 0:
                self.total_time                += self.quantum
                scheduled.remaining_burst_time -= self.quantum
                self.__update_print_str(scheduled.job_number, self.quantum)

            # update next job index (and wrap around if necessary)
            self.fifo_srtn_current = (self.fifo_srtn_current + 1) % len(self.jobs)

    def scheduler_srtn(self):
        scheduled = self.jobs[0] # the job that arrived first is at index 0, since we sort the job list when we read it in.
        while len(self.jobs) != len(self.completed_jobs):
            if not self.__scheduled_job_has_arrived(scheduled):
                continue

            scheduled.update_first_run(self.total_time)
            # update ticker/job
            if scheduled.remaining_burst_time > 0:
                self.__update_print_str(scheduled.job_number, 1)
                scheduled.remaining_burst_time -= 1
                self.total_time                += 1
                # if scheduled job is complete, add it to list
                if scheduled.remaining_burst_time == 0:
                    scheduled.completion_time = self.total_time
                    self.completed_jobs.append(scheduled)
                # each tick, find the next job whose arrival time is either before the current one or at the current tick, with a remaining burst time > 0.
                filtered_jobs = list(filter(lambda job: job.arrival_time <= self.total_time and job.remaining_burst_time > 0, self.jobs))
                # if another job remains, schedule it. otherwise, continue with the currently scheduled job.
                scheduled     = min(filtered_jobs, key=lambda job: job.remaining_burst_time) if len(filtered_jobs) else scheduled

    def stat(self):
        if len(self.completed_jobs):
            self.completed_jobs.sort(key=lambda job: job.job_number)
            print("\n".join(f"Job {job.job_number:3d} -- Turnaround {job.calculate_turnaround_time():3.2f}  Wait {job.calculate_wait_time():3.2f}" for job in self.completed_jobs))
            turnaround_average = sum([job.calculate_turnaround_time() for job in self.completed_jobs]) / len(self.completed_jobs)
            waiting_average    = sum([job.calculate_wait_time() for job in self.completed_jobs])      / len(self.completed_jobs)
            print(f"Average -- Turnaround {turnaround_average:3.2f}  Wait {waiting_average:3.2f}")
    
    def __update_print_str(self, job_number, repeat):
        self.print_str += ("[--]" if job_number == -1 else f"[P{job_number}]") * repeat

    # returns false if the current job has not yet arrived, otherwise true. used for ticker progression.
    def __scheduled_job_has_arrived(self, job):
        if self.total_time < job.arrival_time:
            self.total_time += 1
            self.__update_print_str(-1, 1)
            return False
        return True

def main():
    parser = ArgumentParser()
    parser.add_argument("file", help="the job file to be used", type=str, default="job_list.txt")
    parser.add_argument("-p", "--algorithm", help="the scheduling algorithm to be used", required=False, type=str.lower, default="fifo", choices=["fifo", "rr", "srtn"])
    parser.add_argument("-q",  "--quantum", help="the quantum to be used (required for rr). defaults to 1", required=False, type=int, default=1)
    args = parser.parse_args()

    scheduler           = Scheduler(args.quantum, Job.read_jobs(args.file))
    scheduler.scheduler = {"rr": scheduler.scheduler_rr, "srtn": scheduler.scheduler_srtn}.get(args.algorithm, scheduler.scheduler_fifo)
    scheduler.start_scheduler()
    scheduler.stat()

if __name__ == "__main__":
    main()