from mrjob.job import MRJob

class MRROrderValue(MRJob):
  def mapper(self,_,line):
    (client_id,_,value) = line.split(',')
    yield client_id, float(value) 
  def reducer(self,client_id,value):
    yield client_id, sum(value)

if __name__ == '__main__':
  MRROrderValue.run()