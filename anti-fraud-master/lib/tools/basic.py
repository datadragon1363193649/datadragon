#! /usr/bin/env python
# -*- encoding: utf-8 -*-

class ZnCharProcess(object):
    """
    中文字符的基本处理函数
    """
    def isChinese(self, uchar):
        """判断一个unicode是否是汉字"""  
        if uchar >= u'\u4e00' and uchar<=u'\u9fa5':  
            return True  
        else:  
            return False 

    def isNumber(self, uchar):  
        """判断一个unicode是否是数字"""  
        if uchar >= u'\u0030' and uchar<=u'\u0039':  
            return True  
        else:  
            return False  
    
    def isAlphabet(self, uchar):  
        """判断一个unicode是否是英文字母"""  
        if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):  
            return True  
        else:  
            return False 

    def isOther(self, uchar):  
        """判断是否非汉字，数字和英文字符"""  
        if not (self.isChinese(uchar) or self.isNumber(uchar) or self.isAlphabet(uchar)):  
            return True  
        else:  
            return False 
    
    def B2Q(self, uchar):  
        """半角转全角"""  
        inside_code=ord(uchar)  
        if inside_code<0x0020 or inside_code>0x7e: #不是半角字符就返回原来的字符  
            return uchar  
        if inside_code==0x0020: #除了空格其他的全角半角的公式为:半角=全角-0xfee0  
            inside_code=0x3000  
        else:  
            inside_code+=0xfee0  
        return unichr(inside_code)

    def Q2B(self, uchar):  
        """全角转半角"""  
        inside_code=ord(uchar)  
        if inside_code==0x3000:  
            inside_code=0x0020  
        else:  
            inside_code-=0xfee0  
            if inside_code<0x0020 or inside_code>0x7e: #转完之后不是半角字符返回原来的字符  
                return uchar  
        return unichr(inside_code)

    def stringQ2B(self, ustring):  
        """把字符串全角转半角"""  
        return "".join([self.Q2B(uchar) for uchar in ustring])  
  
    def uniform(self, ustring):  
        """格式化字符串，完成全角转半角，大写转小写的工作"""  
        return self.stringQ2B(ustring).lower()  


    def string2List(self, ustring):  
        """将ustring按照中文，字母，数字分开"""  
        retList=[]  
        utmp=[]  
        for uchar in ustring:  
            if self.isOther(uchar):  
                if len(utmp)==0:  
                    continue  
                else:  
                    retList.append("".join(utmp))  
                    utmp=[]  
            else:  
                utmp.append(uchar)  
        if len(utmp)!=0:  
            retList.append("".join(utmp))  
        return retList


    def isTerminator(self, ch):
        """用于切割段落成句子
        """
        return ch in (u'!', u'?', u',', u';', u'.', u'！', u'？', u'，', u'。', u'…')
    
    def split_into_sentences(self, line):
        tokens = []
        en_token = []

        def close_token(token):
            if token:
                tokens.append(''.join(token))
                del(token[:])

        for c in line:
            if self.isTerminator(c):
                # close current token
                if not tokens: continue
                close_token(en_token)
                #tokens.append(c)
                yield tokens
                tokens = []
            elif self.isOther(c):
                close_token(en_token)
                #tokens.append(c)
            elif self.isChinese(c):
                close_token(en_token)
                tokens.append(c)
            elif c == u' ' or c == u'\t':
                close_token(en_token)
            else:
                en_token.append(c)
                pass
        if tokens:
            yield tokens

if __name__ == '__main__':
    z = ZnCharProcess()
    a = z.split_into_sentences(u'服务员态度也不好，一副要倒闭的样子，人均100，就算没别的地方去也不要去！')
    for i in a:
        print ''.join(i)
