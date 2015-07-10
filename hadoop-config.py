#!/usr/bin/env python
#

import sys;
import re;

def print_opts(m, c):
	m_j = int(0.8 * m)
	r_j = int(2 * m_j)
	r = 2 * m
	s_min = m
	s_max = m * c
	n_res = m * c

	print  \
	"  --mapred-key-value mapreduce.map.java.opts=-Xmx{}m".format(m_j), "\n", \
	"  --mapred-key-value mapreduce.reduce.java.opts=-Xmx{}m".format(r_j), "\n", \
	"  --mapred-key-value mapreduce.map.memory.mb={}".format(m), "\n", \
	"  --mapred-key-value mapreduce.reduce.memory.mb={}".format(r), "\n", \
	"  --yarn-key-value yarn.scheduler.minimum-allocation-mb={}".format(s_min), "\n", \
	"  --yarn-key-value yarn.scheduler.maximum-allocation-mb={}".format(s_max), "\n", \
	"  --yarn-key-value yarn.nodemanager.resource.memory-mb={}".format(n_res)


def main(argv):
	m = 1440
	c = 8

	if len(argv) == 1:
		# no arguments
		# do nothing
		pass
	elif len(argv) == 2:
		# has map
		m = argv[1]
		if "g" in m:
			m = re.sub("g", "*1024", m)
		m = eval(m)
		m = int(m)
		c = 1
		c = int(c)
	elif len(argv) == 3:
		# has m and c
		m = argv[1]
		if "g" in m:
			m = re.sub("g", "*1024", m)
		m = eval(m)
		m = int(m)
		c = argv[2]
		c = int(c)
	
	print_opts(m, c)



if __name__ == "__main__":
	main(sys.argv)
