#!/usr/bin/env python3

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass

class Job:
    def __init__(self, arrival_time, run_time, job_number):
        self.arrival_time = arrival_time
        self.remaining_burst_time = run_time
        self.total_run_time = run_time
        self.job_number = job_number
        # set by scheduling algorithm
        self.first_time_running = -1
        self.completion_time = -1
        self.is_first_run = True

    def get_first_time_running(self):
        return self.first_time_running
    def set_first_time_running(self, time):
        self.first_time_running = time
    def is_first_time_running(self):
        return self.is_first_run
    def set_is_first_time_running(self, value):
        self.is_first_run = value
    def get_completion_time(self):
        return self.completion_time
    def set_completion_time(self, time):
        self.completion_time = time
    def update_completion_time_by(self, diff):
        self.completion_time += diff
    def get_remaining_burst_time(self):
        return self.remaining_burst_time
    def set_remaining_burst_time(self, time):
        self.remaining_burst_time = time
    def update_remaining_burst_time_by(self, diff):
        self.remaining_burst_time += diff
    def get_arrival_time(self):
        return self.arrival_time
    def get_job_number(self):
        return self.job_number
    # turnaround time = completion time - arrival time (time between job arrival and job completion)
    def compute_turnaround_time(self):
        return self.completion_time - self.arrival_time
    # response time   = first run time  - arrival time (time until a job starts producing results)
    def compute_response_time(self):
        return self.first_time_running - self.arrival_time
    # wait time       = turnaround time - run time   (time spent in the ready queue waiting to run)
    def compute_wait_time(self):
        return self.compute_turnaround_time() - self.total_run_time
    def __str__(self):
        return f"-------- Job {self.job_number} --------\n" + f"Arrival Time: {self.arrival_time}\n" + f"First Time Running: {self.first_time_running}\n" + f"Completion Time: {self.completion_time}\n" + f"Run (Burst) Time: {self.total_run_time}\n"
    def __repr__(self):
        return f"Job(number = {self.job_number}, arrival = {self.arrival_time}, burst = {self.remaining_burst_time})"
    def __lt__(self, other):
        return self.get_arrival_time() < other.get_arrival_time()
    @staticmethod
    def read_jobs(input_file):
        with open(input_file, "r") as file:
            lines = [line.split(" ") for line in file.readlines()]
            jobs = []
            for index, job in enumerate(lines):
                jobs.append(Job(run_time=int(job[0]), arrival_time=int(job[1]), job_number=index))
            jobs.sort(key= lambda job: job.arrival_time)
        return jobs

class Scheduler:
    def __init__(self, quantum):
        self.job_list = []
        self.completed_jobs = []
        self.num_jobs = 0
        self.num_jobs_completed = 0
        self.schedule = self.scheduler_fifo
        self.quantum = quantum
        self.total_time = 0
        self.print_str = ""
    
    def submit_jobs(self, jobs):
        for job in jobs:
            self.job_list.append(job)
            self.num_jobs += 1

    def set_scheduling_func(self, schedule_func):
        self.schedule = schedule_func

    def start_scheduler(self):
        self.schedule()
    
    def scheduler_fifo(self):

        if len(self.job_list) == 0:
            return

        current_job_index = 0

        while self.num_jobs != self.num_jobs_completed:
            scheduled = self.job_list[current_job_index]
            if self.total_time < scheduled.get_arrival_time():
                self.total_time += 1
                self.print_str += "[--]"
                continue
            
            scheduled.set_first_time_running(self.total_time)
            self.total_time += scheduled.get_remaining_burst_time()
            scheduled.set_completion_time(self.total_time)
            self.print_str += f"[P{scheduled.get_job_number()}]" * scheduled.get_remaining_burst_time()
            scheduled.set_remaining_burst_time(0)
            self.num_jobs_completed += 1

            # update job lists
            self.job_list[current_job_index] = scheduled
            self.completed_jobs.append(scheduled)

            current_job_index += 1

        
    def scheduler_rr(self):
        if len(self.job_list) == 0:
            return
        current_job_index = 0
        while self.num_jobs != self.num_jobs_completed:
            scheduled = self.job_list[current_job_index]
            if self.total_time < scheduled.get_arrival_time():
                self.total_time += 1
                self.print_str += "[--]"
                continue
            
            # if first time running, set first run time.
            if scheduled.is_first_time_running():
                scheduled.set_first_time_running(self.total_time)
                scheduled.set_is_first_time_running(False)

            # if remaining burst in (0, self.quantum], run to completion.
            if scheduled.get_remaining_burst_time() <= self.quantum and scheduled.get_remaining_burst_time() > 0:
                self.total_time += scheduled.get_remaining_burst_time()
                self.print_str += f"[P{scheduled.get_job_number()}]" * scheduled.get_remaining_burst_time()
                scheduled.set_completion_time(self.total_time)
                scheduled.set_remaining_burst_time(0)
                self.completed_jobs.append(scheduled)
                self.num_jobs_completed += 1
            # otherwise, run a slice of it.
            elif scheduled.get_remaining_burst_time() > 0:
                self.total_time += self.quantum
                scheduled.update_remaining_burst_time_by(-self.quantum)
                self.print_str += f"[P{scheduled.get_job_number()}]" * self.quantum

            # if we're at the end of our list, wrap around.
            if current_job_index == self.num_jobs - 1:
                current_job_index = 0
            else:
                current_job_index += 1

    def scheduler_srtn(self):
        if len(self.job_list) == 0:
            return
        # sort by arrival time then by remaining burst time
        scheduled = min(self.job_list, key=lambda job: job.get_arrival_time())
        while self.num_jobs != self.num_jobs_completed:
            if self.total_time < scheduled.get_arrival_time():
                self.total_time += 1
                self.print_str += "[--]"
                continue

            # if first time running, set first run time.
            if scheduled.is_first_time_running():
                scheduled.set_first_time_running(self.total_time)
                scheduled.set_is_first_time_running(False)

            # update ticker/job
            if scheduled.get_remaining_burst_time() > 0:
                scheduled.update_remaining_burst_time_by(-1)
                self.total_time += 1
                self.print_str += f"[P{scheduled.get_job_number()}]"
                # find the next job whose arrival was either before the current one or at the current tick.
                filtered_jobs = list(filter(lambda job: job.get_arrival_time() <= self.total_time and job.get_remaining_burst_time() > 0, self.job_list))
                # if job is complete, add it to list
                if scheduled.get_remaining_burst_time() == 0:
                    scheduled.set_completion_time(self.total_time)
                    self.completed_jobs.append(scheduled)
                    self.num_jobs_completed += 1

                # if another job remains, schedule it. otherwise, continue with the currently scheduled job.
                if len(filtered_jobs) > 0:
                    scheduled = min(filtered_jobs, key=lambda job: job.get_remaining_burst_time())

            """
            Find jobs that:
            1. are shorter to/equal in length than the current one
            2. have a burst time > 0
            3. are not equal to the current one (maybe if we leave this out, it will schedule itself.) 
            """


    def stat(self):
        self.completed_jobs.sort(key=lambda job: job.job_number)
        for job in self.completed_jobs:
            # print(job)
            print(f"Job {job.job_number:3d} -- Turnaround {job.compute_turnaround_time():3.2f}  Wait {job.compute_wait_time():3.2f}")
        turnaround_average = sum([job.compute_turnaround_time() for job in self.completed_jobs]) / len(self.completed_jobs)
        waiting_average = sum([job.compute_wait_time() for job in self.completed_jobs]) / len(self.completed_jobs)
        print(f"Average -- Turnaround {turnaround_average:3.2f}  Wait {waiting_average:3.2f}")
        # print(self.print_str)

def main():
    # argument setup

    debug = False

    @dataclass
    class NamespaceDebug:
        file: str
        algorithm: str
        quantum: int

    if debug:
       args = NamespaceDebug(file="/Users/remi/Documents/School/csc453/assignment2/srtn1.txt", algorithm="srtn", quantum=4) 
    else:
        parser = ArgumentParser()
        parser.add_argument("file", type=str, default="job_list.txt")
        # file: str = "./fcfs1.txt"
        parser.add_argument("-p", "--algorithm", required=False, type=str.lower, default="fifo", choices=["fifo", "rr", "srtn"])
        parser.add_argument("-q",  "--quantum", required=False, type=int, default=1)
        args = parser.parse_args()
    
    scheduler = Scheduler(args.quantum)

    if args.algorithm == "rr":
        scheduler.set_scheduling_func(scheduler.scheduler_rr)
    elif args.algorithm == "srtn":
        scheduler.set_scheduling_func(scheduler.scheduler_srtn)
    else:
        scheduler.set_scheduling_func(scheduler.scheduler_fifo)
    
    scheduler.submit_jobs(Job.read_jobs(args.file))
    scheduler.start_scheduler()
    scheduler.stat()

if __name__ == "__main__":
    main()