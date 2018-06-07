# coding=utf-8
class Trie:
    leftroot = {}
    rightroot={}
    END = '/'
    phnlist = []

    def add(self, word):
        leftnode = self.leftroot
        rightroot=self.rightroot
        for c in word:
            leftnode = leftnode.setdefault(c, {})
        leftnode[self.END] = None
        word=word[::-1]
        for c in word:
            rightroot = rightroot.setdefault(c, {})
        rightroot[self.END] = None
    def run_dic(self,node,word):
        if isinstance(node, dict):
            print node.keys()
            for x in range(len(node)):
                if self.END in node.keys()[x]:
                    self.phnlist.append(word)
                word+=node.keys()[x]
                # print word
                node1=node[node.keys()[x]]
                self.run_dic(node1, word)
                word=word[:-1]

    def find(self, word,node):
        for c in word:
            if c not in node:
                return False
            node = node[c]
        print c,node
        self.run_dic(node,word)
        # if self.END in node:
        #     phnlist.append()
        # print 'target',self.phnlist
if __name__ == '__main__':
    t=Trie()
    sfile = '/Users/ufenqi/Documents/relation'
    plist = []
    with open(sfile, 'r') as fp:
        for line in fp:
            p0, _, _, p = line.strip().split(',')
            plist.append(p0)
    for pl in plist:
        if pl.strip() and pl.count('*') * 1.0 / len(pl) >= 0.5:
            continue
        t.add(pl)
    s='1521***7297'
    # s='15218987297'
    if '*' not in s:
        t.find(s.strip(), t.leftroot)
        l = t.phnlist
        print l
    else:
        al=s.strip().split('*')
        lefts=al[0]
        rights=al[-1][::-1]
        t.find(lefts,t.leftroot)
        l=t.phnlist
        t.phnlist=[]
        t.find(rights,t.rightroot)
        r=[]
        for phn in t.phnlist:
            if phn[::-1] in l:
                print phn[::-1]
