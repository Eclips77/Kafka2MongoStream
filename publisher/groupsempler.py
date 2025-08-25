from sklearn.datasets import fetch_20newsgroups
class NewsgroupSampler:
    """""
    Samples messages from selected 'interesting' and 'not interesting' newsgroup categories using the 20 Newsgroups dataset.
    
    """
    def __init__(self):
        self.interesting_categories = [
            'alt.atheism',
            'comp.graphics',
            'comp.os.ms-windows.misc',
            'comp.sys.ibm.pc.hardware',
            'comp.sys.mac.hardware',
            'comp.windows.x',
            'misc.forsale',
            'rec.autos',
            'rec.motorcycles',
            'rec.sport.baseball'
        ]
        self.not_interesting_categories = [
            'rec.sport.hockey',
            'sci.crypt',
            'sci.electronics',
            'sci.med',
            'sci.space',
            'soc.religion.christian',
            'talk.politics.guns',
            'talk.politics.mideast',
            'talk.politics.misc',
            'talk.religion.misc'
        ]
        self.interesting = fetch_20newsgroups(subset='all', categories=self.interesting_categories)
        self.not_interesting = fetch_20newsgroups(subset='all', categories=self.not_interesting_categories)

    def get_sample(self, total=20)-> dict:
        """
        Returns a sample of messages from both 'interesting' and 'not interesting' categories.

        Args:
            total (int, optional): Total number of messages to sample, split equally between both categories. Defaults to 20.

        Returns:
            dict: A dictionary containing two keys:
                - "interesting": List of sampled interesting messages.
                - "not_interesting": List of sampled not interesting messages.
        """
        half = total // 2
        interesting_msgs = self.interesting.data[:half]
        not_interesting_msgs = self.not_interesting.data[:half]
        return {
            "interesting": interesting_msgs,
            "not_interesting": not_interesting_msgs
        }

# if __name__ == "__main__":
    # x = NewsGroupSampler()
    # z = x.get_sample()
    # print(z)