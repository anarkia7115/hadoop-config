#!/usr/bin/env python
#

import sys;
import re;

def print_opts(m, c):
	m_j = int(0.8 * m)
	r_j = int(2 * m_j)
	r = 2 * m
	s_min = m
	s_max = max(m * c, m + r)
	n_res = max(m * c, m + r)

	print  \
        "[\n" \
        "\t{\n" \
	"\t\t\"--mapred-key-value\", \n" ,\
        "\t\t\"mapreduce.map.java.opts=-Xmx{}m\",\n".format(m_j), \
	"\t\t\"--mapred-key-value\", \n" ,\
        "\t\t\"mapreduce.reduce.java.opts=-Xmx{}m\",\n".format(r_j), \
	"\t\t\"--mapred-key-value\", \n" ,\
        "\t\t\"mapreduce.map.memory.mb={}\",\n".format(m), \
	"\t\t\"--mapred-key-value\", \n" ,\
        "\t\t\"mapreduce.reduce.memory.mb={}\",\n".format(r), \
	"\t\t\"--yarn-key-value\", \n" ,\
        "\t\t\"yarn.scheduler.minimum-allocation-mb={}\",\n".format(s_min), \
	"\t\t\"--yarn-key-value\", \n" ,\
        "\t\t\"yarn.scheduler.maximum-allocation-mb={}\",\n".format(s_max), \
	"\t\t\"--yarn-key-value\", \n" ,\
        "\t\t\"yarn.nodemanager.resource.memory-mb={}\"\n".format(n_res), \
	"\t\t\"--mapred-key-value\", \n" ,\
        "\t\t\"mapreduce.task.timeout=0\"\n", \
        "\t}\n", \
        "]\n"


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
