import unittest

class TransformTest(unittest.TestCase):
    def test_first_name(self):
        self.assertEqual(3,3)

    def test_second_name(self):
        self.assertTrue('PYTHON'.isupper())

if __name__ == '__main__':
    unittest.main()

