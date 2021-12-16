# https://pypi.org/project/mrjob/
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRDataMining(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_images_name, reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_make_counts_key, reducer = self.reducer_output_words)
        ]

    def mapper_images_name(self, _, line):
        words = re.split(",", line)
        yield words[0].lower().strip(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        yield '%03d'%int(count), word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRDataMining.run()