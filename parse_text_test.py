# -*- coding: utf-8 -*-

import unittest
from parse_text import check_for_url

class URLCheckTestCases(unittest.TestCase):
    
    def test_1(self):
        text1 = '\"EuroRSCG: Interesting - Men splurging on clothing signal firming U.S. growth - http://t.co/OBKSFBEB - (via @reuters) #eurorscg #shopping...'
        self.assertEqual(check_for_url(text1), 'http://t.co/OBKSFBEB')

    def test_2(self):
        text2 = 'George W. Bush did more than fuck our economy.. #ShittiestPresidentAward'
        self.assertEqual(check_for_url(text2), None)

    def test_3(self):
        text3 = 'THIS IS UNACCEPTABLE! How The House Republicansâ€™ Transportation Bill Hurts Low-Income Minorities http://t.co/WrhKTjvQ'
        self.assertEqual(check_for_url(text3), 'http://t.co/WrhKTjvQ')

    def test_4(self):
        text4 = 'First new nuclear reactors OK\'d in over 30 years http://t.co/xmqYmzIo via @CNNMoney'
        self.assertEqual(check_for_url(text4), 'http://t.co/xmqYmzIo')

    def test_5(self):
        text5 = 'oextrs: Film Finance attorney John Cones shares some "Chain of Title" info you might be able to use. http://fb.me/RfIGdzw4'
        self.assertEqual(check_for_url(text5), 'http://fb.me/RfIGdzw4')

    def test_6(self):
        text6 = 'Eurozone finance ministers are to meet tomorrow to discuss Greek bailout deal. #Eurocrisis'
        self.assertEqual(check_for_url(text6), None)

    def test_7(self):
        text7 = 'Facebook graffiti artist could be worth $500 million yhoo.it/ypM4yy'
        self.assertEqual(check_for_url(text7), 'yhoo.it/ypM4yy')

if __name__ == '__main__':
    unittest.main()
