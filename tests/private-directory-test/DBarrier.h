/**
 * Distributed Barrier
 *
 * Synchronize multiple threads/processes on different machine.
 *
 * See `:/tests/test_dbarrier.cc` and `:/tests/tset_dbarrier.sh` for usage.
 */
#pragma once

#include <csignal>
#include <mutex>
#include <cstring>
#include <bits/stdc++.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "log/logging.h"

#define MAXLINE 1024

using std::mutex;

class UDPListener {
public:
	void check_err_exit(int r, const char *msg)
	{
		if (r < 0) {
			perror(msg);
			exit(EXIT_FAILURE);
		}
	}
	int wait_on_port(int port)
	{
		// https://www.geeksforgeeks.org/udp-server-client-implementation-c/
        FALCON_LOG_PRINTF(LOG_INFO, "Waiting on port %d\n", port);

		int sockfd;
		char buffer[MAXLINE];
		struct sockaddr_in servaddr, cliaddr;
		// Creating socket file descriptor
		sockfd = socket(AF_INET, SOCK_DGRAM, 0);
		check_err_exit(sockfd, "socket creation failed");

		memset(&servaddr, 0, sizeof(servaddr));
		memset(&cliaddr, 0, sizeof(cliaddr));

		// Filling server information
		servaddr.sin_family = AF_INET; // IPv4
		servaddr.sin_addr.s_addr = INADDR_ANY;
		servaddr.sin_port = htons(port);

		// Bind the socket with the server address
		int r = bind(sockfd, (const struct sockaddr *)&servaddr,
			     sizeof(servaddr));
		check_err_exit(r, "bind failed");

		socklen_t len;
		int n;
		len = sizeof(cliaddr);
		n = recvfrom(sockfd, (char *)buffer, MAXLINE,
			     MSG_WAITALL, ( struct sockaddr *) &cliaddr,
			     &len);
		close(sockfd);
		return n;
	}
};

class DBarrier {
public:
	DBarrier()
	{
		runnable = false;
	}
	~DBarrier() {
	}
	virtual void wait()
	{
		while (!runnable)
			cpu_relax();
	}
protected:
	volatile bool runnable;
	static inline void cpu_relax()
	{
		//asm volatile("rep; nop" ::: "memory");  // For x86 only.
#if defined(__ARM_ARCH_7A__) || defined(__aarch64__)
    		asm volatile("yield" ::: "memory");
#else
    		asm volatile("nop" ::: "memory"); // ARMv5
#endif
	}
};

/// Use SIGUSR1 signal to synchronize
class DBarrierOnSignal: public DBarrier {
public:
	DBarrierOnSignal()
	{
		singleton = this;
		std::signal(SIGUSR1, DBarrierOnSignal::signal_handler);
	}
	~DBarrierOnSignal()
	{
		// Restore the default handler
		std::signal(SIGUSR1, SIG_DFL);
	}
private:
	static void signal_handler(int signal)
	{
		singleton->runnable = true;
	}
	static DBarrierOnSignal *singleton;
};

/// Use UDP to synchronize
class DBarrierOnUDP: public DBarrier {
public:
	DBarrierOnUDP(int _port) : port(_port), first(true) { }
	~DBarrierOnUDP() { }

	void wait() override
	{
		// Choose the first waiter.
		mtx.lock();
		bool myfirst = first;
		if (first) first = false;
		mtx.unlock();

		// The first waiter needs to listen to UDP.
		if (myfirst) {
			udp.wait_on_port(port);
			FALCON_LOG_PRINTF(LOG_INFO, "receive signal\n");
			runnable = true;
			return;
		}
		// Others wait for the variable.
		while (!runnable)
			cpu_relax();
	}

	void run() {
		runnable = true;
	}

    void reset() {
        runnable = false;
		first = true;
    }
private:
	UDPListener udp;
	mutex mtx;
	int port;
	bool first;
};
