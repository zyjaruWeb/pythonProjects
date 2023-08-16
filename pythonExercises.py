
# load array
# if 19 appears twice& 5 appear at least 3 (equal or more than 3), then True, else false
# print outpu

def test(nums):
    return nums.count(19) == 2 and nums.count(5) >=3
nums = []
print("Original list: ")
print(nums)
print("Check two occurences of nineteen and at least three occurences of five in the said list")
print(test(nums))