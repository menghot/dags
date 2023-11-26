import re
import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        text = "This is a sample string with some numbers like 12345 and 67890."

        pattern = r'\d+'  # \d matches any digit, + matches one or more occurrences

        # Using re.findall to find all matches in the text based on the pattern
        matches = re.findall(pattern, text)

        print(re.match(r'.*termination reason: \w', ""))

        # Printing the matches found
        print(matches)

        text = "termination reason: Completed asds"

        # Regular expression pattern to extract 'Completed'
        pattern = r'termination reason:\s*(\w+)'

        # Using re.search to find the pattern in the text
        match = re.search(pattern, text)

        # Extracting the matched group
        if match:
            completed_reason = match.group(1)
            print(completed_reason)
        else:
            print("Pattern not found.")



        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
