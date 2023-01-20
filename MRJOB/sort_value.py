from mrjob.job import MRJob
from mrjob.step import MRStep

class MRROrderValue(MRJob):

  def steps(self):
    return [MRStep(mapper=self.mapper, reducer=self.reducer), 
    MRStep(mapper=self.mapper_sorted, reducer=self.reducer_sorted)]

  def mapper(self,_,line):
    (client_id,_,value) = line.split(',')
    yield client_id, float(value) 

  def reducer(self,client_id,value):
    yield client_id, sum(value)

  def mapper_sorted(self,id, count):
    yield None, (count, id)

  def reducer_sorted(self,_, value):
    for var1, var2 in sorted(value):
      yield var1, var2

if __name__ == '__main__':
  MRROrderValue.run()