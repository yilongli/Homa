#include <iostream>
#include <vector>

#include <signal.h>

#include <Homa/Drivers/DPDK/DpdkDriver.h>
#include <Cycles.h>
#include <TimeTrace.h>
#include <docopt.h>

#include "Output.h"

static const char USAGE[] = R"(HomaRpcBench dpdk_test.

    Usage:
        dpdk_test [options] <iface> (--server | <server_address>)

    Options:
        -h --help           Show this screen.
        --version           Show version.
        --timetrace         Enable TimeTrace output [default: false].
)";

volatile sig_atomic_t INTERRUPT_FLAG = 0;
void
sig_int_handler(int sig)
{
    INTERRUPT_FLAG = 1;
}

int
main(int argc, char* argv[])
{
    std::map<std::string, docopt::value> args =
            docopt::docopt(USAGE, {argv + 1, argv + argc},
                    true,                       // show help if requested
                    "HomaRpcBench dpdk_test");  // version string

    std::string iface = args["<iface>"].asString();
    bool isServer = args["--server"].asBool();
    std::string server_address_string;
    if (!isServer) {
        server_address_string = args["<server_address>"].asString();
    }

    Homa::Drivers::DPDK::DpdkDriver driver(iface.c_str());

    if (isServer) {
        std::cout << Homa::Util::ipToString(driver.getLocalAddress())
                  << std::endl;
        while (true) {
            if (INTERRUPT_FLAG) {
                break;
            }
            Homa::Driver::Packet* incoming[10];
            uint32_t receivedPackets;
            do {
                receivedPackets = driver.receivePackets(10, incoming);
            } while (receivedPackets == 0);
            Homa::Driver::Packet* pong = driver.allocPacket();
            pong->length = 100;
            driver.sendPacket(pong, incoming[0]->sourceIp, 0);
            driver.releasePackets(incoming, receivedPackets);
            driver.releasePackets(&pong, 1);
        }
    } else {
        Homa::IpAddress server_address =
                Homa::Util::stringToIp(server_address_string.c_str());
        std::vector<Output::Latency> times;
        for (int i = 0; i < 100000; ++i) {
            uint64_t start = PerfUtils::Cycles::rdtsc();
            PerfUtils::TimeTrace::record(start, "START");
            Homa::Driver::Packet* ping = driver.allocPacket();
            PerfUtils::TimeTrace::record("allocPacket");
            ping->length = 100;
            PerfUtils::TimeTrace::record("set ping args");
            driver.sendPacket(ping, server_address, 0);
            PerfUtils::TimeTrace::record("sendPacket");
            driver.releasePackets(&ping, 1);
            PerfUtils::TimeTrace::record("releasePacket");
            Homa::Driver::Packet* incoming[10];
            uint32_t receivedPackets;
            do {
                receivedPackets = driver.receivePackets(10, incoming);
                PerfUtils::TimeTrace::record("receivePackets");
            } while (receivedPackets == 0);
            driver.releasePackets(incoming, receivedPackets);
            PerfUtils::TimeTrace::record("releasePacket");
            uint64_t stop = PerfUtils::Cycles::rdtsc();
            times.emplace_back(PerfUtils::Cycles::toSeconds(stop - start));
        }
        if (args["--timetrace"].asBool()) {
            PerfUtils::TimeTrace::print();
        }
        std::cout << Output::basicHeader() << std::endl;
        std::cout << Output::basic(times, "DpdkDriver Ping-Pong") << std::endl;
    }

    return 0;
}