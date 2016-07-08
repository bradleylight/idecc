# python3
# program that calculates the  median degree of a venmo transaction graph
# insight data engineering coding challenge, William Light, 2016

import sys, json, time, datetime, statistics

# initialize 

inputfile = sys.argv[1]
outputfile = sys.argv[2]

max_created_ts = 0

Ndict = {}
Edict = {}

# define

# open files, close files (after main loop)

fr = open(inputfile, 'rU')
fw = open(outputfile, 'w')

# main loop
#   read from input file, parse line, process, write to output file
#   (put just one line at a time from the input file into memory)

for line in fr:
  parsed = json.loads(line) # fields are 'actor', 'target', 'created_time'
  actor, target, cts = parsed['actor'], parsed['target'], parsed['created_time']
  created_ts = time.mktime(datetime.datetime.strptime(cts, "%Y-%m-%dT%H:%M:%SZ").timetuple())
  dt = created_ts - max_created_ts    # elapsed time in seconds since prior transactions
  vv = tuple(sorted([parsed['actor'], parsed['target']])) # "timeless" edge key for Edict

  if dt > -60:
#     add vertices (0/1/2), add/update 1 edge, update 2 vertex degrees each (if edge added)
    if not (actor in Ndict):
      Ndict[actor] = 0       # degree is initially zero for a new vertex
    if not (target in Ndict):
      Ndict[target] = 0       # degree is initially zero for a new vertex
#     add edges and/or replace old with new if already existing (updates the timestamp)
    if vv in Edict:
      if created_ts > Edict.get(vv):
        del Edict[vv]
        Edict[vv] = created_ts
    else:
      Edict[vv] = created_ts
      Ndict[actor] += 1
      Ndict[target] += 1
    if dt > 0:
      max_created_ts += dt
      # prune old edges and update 2 vertex degrees each, prune vertices with degree 0
      del_list = []
      for k, v in Edict.items():
        if max_created_ts - v >= 60:
          Ndict[k[0]] -= 1
          Ndict[k[1]] -= 1
          del_list.append(k)
      for k in del_list:
        del Edict[k]
      del_list = []
      for k, v in Ndict.items():
        if v == 0:
          del_list.append(k)
      for k in del_list:
        del Ndict[k]
    # update rolling_median
    rolling_median = statistics.median(list(Ndict.values()))
  # write rolling_median to output file
  fw.write('%.2f' % rolling_median + "\n")

  # fw.write(line)        # as a first test just write the input line to the output file
  # fw.write('%.2f' % dt + "\n")
  # fw.write(repr(Ndict) + "\n")
  # fw.write(actor + "\n")
  # fw.write(target + "\n")
  # fw.write(cts + "\n")
  # fw.write(str(created_ts) + "\n")
  # fw.write(datetime.datetime.fromtimestamp(created_ts).strftime('%Y-%m-%d %H:%M:%S') + "\n")

fw.close()
fr.close()
