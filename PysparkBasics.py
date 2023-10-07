# Databricks notebook source
# MAGIC %md
# MAGIC ## Difference Between Lists Vs Tuples Vs Sets
# MAGIC  Vs Dictionaries in Python
# MAGIC
# MAGIC ### Lists:
# MAGIC
# MAGIC Ordered: Lists are ordered collections, which means they store elements in a specific sequence, and you can access elements by their position (index).
# MAGIC Mutable: Lists are mutable, meaning you can change, add, or remove elements after creation.
# MAGIC Duplicate Elements: Lists can contain duplicate elements.
# MAGIC Syntax: Defined using square brackets [ ].
# MAGIC Example:
# MAGIC
# MAGIC my_list = [1, 2, 3, 2, 4]
# MAGIC
# MAGIC ### Tuples:
# MAGIC
# MAGIC Ordered: Tuples are also ordered collections, and they maintain the order of elements like lists.
# MAGIC Immutable: Tuples are immutable, meaning you cannot change their contents once created. This immutability provides some safety and performance benefits.
# MAGIC Duplicate Elements: Tuples can contain duplicate elements.
# MAGIC Syntax: Defined using parentheses ( ).
# MAGIC Example:
# MAGIC
# MAGIC my_tuple = (1, 2, 3, 2, 4)
# MAGIC
# MAGIC ### Sets:
# MAGIC
# MAGIC Unordered: Sets are unordered collections, which means they don't have a specific order for their elements, and you cannot access elements by index.
# MAGIC Unique Elements: Sets only contain unique elements. They automatically remove duplicates.
# MAGIC Mutable (for set): The built-in set type is mutable, meaning you can add or remove elements after creation. However, there's also an immutable version called frozenset.
# MAGIC Syntax: Defined using curly braces { } or the set() constructor.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC my_set = {1, 2, 3, 2, 4}
# MAGIC
# MAGIC ### Dictionaries:
# MAGIC
# MAGIC Unordered (Python 3.6+): Prior to Python 3.6, dictionaries were unordered collections, but starting from Python 3.6, dictionaries maintain insertion order.
# MAGIC Key-Value Pairs: Dictionaries store data as key-value pairs. Each key is unique, and you can access values by their keys.
# MAGIC Mutable: Dictionaries are mutable, meaning you can add, change, or remove key-value pairs after creation.
# MAGIC Syntax: Defined using curly braces { } with key-value pairs separated by colons :.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC my_dict = {'name': 'John', 'age': 30, 'city': 'New York'}
# MAGIC s
# MAGIC In summary, the key differences lie in their order (ordered vs. unordered), mutability (mutable vs. immutable), and uniqueness of elements (allowing duplicates vs. only storing unique elements). The choice of which data structure to use depends on the specific requirements of your program or data manipulation needs.

# COMMAND ----------

a = 10
print(a)

# COMMAND ----------

a = 10
b = 20
print(a+b)

# COMMAND ----------

a = 10
b = 20
c = a
a = b
b = c
print(a,b)

# COMMAND ----------

#Swapping of two numbers 
a,b = 1,2
print(a,b)
a,b = b,a
print(a,b)


# COMMAND ----------

a,b = 10,"srinu"
print(a,b)

# COMMAND ----------

str = "hello"
print(str)
str1 = str[::-1]
print(str1)
print(str == str1)

# COMMAND ----------

# MAGIC %md
# MAGIC # List Operations
# MAGIC ### a list is a built-in data structure that is used to store a collection of items. Lists are ordered, mutable (modifiable), and allow for the storage of heterogeneous data types, meaning you can have elements of different data types within a single list. Lists are defined by enclosing a comma-separated sequence of elements in square brackets [ ].
# MAGIC

# COMMAND ----------

#create a empty list
my_list  = []
print(my_list)

# COMMAND ----------

# create a list wtih initial values
my_list = [1, 2, 3, "Hello", 5.0]
print("Display list",my_list)

# COMMAND ----------

numbers = [1, 2, 3, 4, 5]
numbers

# COMMAND ----------

# List , if we call specific item value, use index, index starts from 0.

# COMMAND ----------

#Accessing elements by index in the list
first_element = numbers[0]
last_element = numbers[-1]
print(first_element,last_element)


# COMMAND ----------

print(numbers[1:4])

# COMMAND ----------

# [1, 2, 3, 4, 5]
print(numbers[:-1])

# COMMAND ----------

list1 = [1,2,3,44,66]
list2 = [4,5,6,44,66]
list3 = list1 + list2
print(list3)

# COMMAND ----------

#Accessing elements by Slicing a list:
sub_list = numbers[1:4]  # Returns [2, 3, 4]
sub_list

# COMMAND ----------

sub_list = numbers[1:]  
sub_list

# COMMAND ----------

sub_list = numbers[:4]  
sub_list

# COMMAND ----------

# [start:endvalue:-1]
list1 = [1,2,3,4,5,6]
list1[::-1]

# COMMAND ----------

string1 = "hello"
print(string1[::-1])
stringlist = ['h','e','e','l','l','o']
print(stringlist[::-1])


# COMMAND ----------


print(numbers[5:0])

# COMMAND ----------

# append to the list
# it will be added to the end of the list
numbers = [1,2,3,4,5]
numbers.append(6)
numbers

# COMMAND ----------

# remvoe the number
numbers.remove(6)
numbers

# COMMAND ----------

numbers = [1,5,6,7,3,7,6,6,6]
print(numbers)
numbers.remove(6)
print(numbers)

# COMMAND ----------

# note remove , delete first occurance of the element
a = [1,2,3,4,5,4,6,4]
a.remove(4)
a

# COMMAND ----------

#Insert an element at a specific index:
numbers = [0,1,2,3,5,6]
numbers.insert(4,9)
numbers

# COMMAND ----------

numbers.insert(3,10)
numbers

# COMMAND ----------

# Remove an element by index:
numbers = [1,2,3,4,5,3,6]
print(len(numbers))
numbers.pop(5)
print(len(numbers))


# COMMAND ----------

#Concatenate two lists:
merge_list = numbers + [6,7]

# COMMAND ----------

merge_list

# COMMAND ----------

#lenght of the list
len(merge_list)

# COMMAND ----------

list1 = [1,2,3,4,5]
for i in list1:
    print(i*i)

# COMMAND ----------

j = [i*i for i in list1]
print(j)
print("type of j is " ,type(j))

# COMMAND ----------

print(i)

# COMMAND ----------

print(range(10))
for i in range(10):
    print(i,end='')

# COMMAND ----------

for list in range(len(merge_list)):
    print(list)

# COMMAND ----------

list1 = [1,2,3,4,5,6]
for i in list1:
    if i in list1:
        print("10 exit")
    else:
        print("not exist")

# COMMAND ----------

# use of if condition to check the specific element
if 200 in merge_list:
    print("2 is vailable")
else:
    print("2 is not available")

# COMMAND ----------

# loop the list of items
range(len(merge_list))

# COMMAND ----------

merge_list

# COMMAND ----------

for i in range(len(merge_list)):
    print(i)

# COMMAND ----------

for i in merge_list:
    print(i)

# COMMAND ----------

merge_list = [1,2,3,4]
newlist = []
for i in merge_list:
    newlist.append(i ** i)
newlist

# COMMAND ----------

newlist = [i**i for i in merge_list]
newlist

# COMMAND ----------

newlist = [i*i for i in merge_list]
newlist

# COMMAND ----------

type(newlist)

# COMMAND ----------

newlist = [1,2,3,4,5,6]

# COMMAND ----------

[i ** 2 for i in newlist]

# COMMAND ----------

newlist

# COMMAND ----------

newlist.sort(reverse=False)
print(newlist)
newlist.sort(reverse=True)
print(newlist)

# COMMAND ----------

my_list = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]
my_list.sort(reverse=True)
print(my_list)


# COMMAND ----------

numbers = [1,4,2,5,3]
sorted(numbers)

# COMMAND ----------

if 5 in numbers:
    print ("5 is inthe lsit")


# COMMAND ----------



# COMMAND ----------

print(numbers)

# COMMAND ----------

indexposition = numbers.index(5)
print(indexposition)

# COMMAND ----------

for number in numbers:
    print(number)

# COMMAND ----------

# even numbers
numbers = [1,2,3,4,5,6,7,8,9,10]
evenlist = []
oddlist = []
for i in numbers:
    if i%2 == 0:
        evenlist.append(i)
    else:
        oddlist.append(i)

        

# COMMAND ----------

print(evenlist)
print(oddlist)

# COMMAND ----------

# even numbers
numbers = [1,2,3,4,5,6,7,8,9,10]
even_numbers = [i for i in numbers if i%2 == 0]
print(even_numbers)


# COMMAND ----------

#Create a shallow copy of a list:
newnumbers = numbers.copy()
newnumbers
numbers

# COMMAND ----------

newnumbers.clear()

# COMMAND ----------

matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
# output [1,2,3,4,5,6,7,8,9]
matrixnew=[]
for i in matrix:
    for j in i:
        matrixnew.append(j)

print(matrixnew)

newmatrix=[]
newmatrix=[j for i in matrix for j in i]
print(newmatrix)


# COMMAND ----------

# create a list of list
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
# output [1,2,3,4,5,6,7,8,9]
for i in matrix:
    print(i)

matrix

# COMMAND ----------

[item for sublist in matrix for item in sublist]

# COMMAND ----------

#You can divide a list into two separate lists, one containing integers and the other containing strings, by iterating through the original list and checking the type of each element. Here's an example:

original_list = [1, 'apple', 2, 'banana', 3, 'cherry']
int_list = []
str_list = []
for list in original_list:
    if isinstance(list,int):
        int_list.append(list)
    elif isinstance(list,str):
         str_list.append(list)
print("integers:",int_list)
print("Strings:",str_list)

# COMMAND ----------

a='hello'
type(a)

# COMMAND ----------

original_list = [1, 'apple', 2, 'banana', 3, 'cherry']
int_list = []
str_list = []
for item in original_list:
    if isinstance(item, int):
        int_list.append(item)
    elif isinstance(item, type(item)):
        str_list.append(item)

print("Integers:", int_list)
print("Strings:", str_list)


# COMMAND ----------

# convert characters to string value from the list
char_list = ['H', 'e', 'l', 'l', 'o']
result_string = ''
for char in char_list:
    result_string = result_string + char # result_string += char
result_string
type(result_string)


# COMMAND ----------

result_string_list = [result_string] 
result_string_list

# COMMAND ----------

#convert list to tuple
result_string = ['hello']
tuple_list = tuple(result_string)
tuple_list

# the output of tuple with single value is ('Hello',) , its behavior of ending with coma(,), it is not for more than one value

# COMMAND ----------

n=5
for i in range(1,n+1):
    for j in range(1,i+1):
        print(j,end='')
    print()
# In the code snippet provided, we used n + 1 in the range for the first half of the pattern because we wanted to include the value n in the pattern
#The print(j, end='') statement is used to print the value of j without adding a newline character (\n) at the end of the printed output. By default, the print() function in Python adds a newline character at the end, which moves the cursor to the next line after printing.

for i in range(n-1,0,-1):
    for j in range(1,i+1):
        print(j,end='')
    print()
#n-1 is the starting value of the range. It's one less than n, so if n is 5, the range starts at 4.
# 0 is the stopping value of the range. The range stops before reaching 0.
# -1 is the step size, which means the numbers are generated in reverse order, subtracting 1 from the previous number.
#For example, if n is 5, range(

# COMMAND ----------

str = 'hello'
for i in range(len('hello')+1):
    print(str[:i])
for i in range(len('hello')-1,0,-1):
    print(str[:i])

# COMMAND ----------

# MAGIC %md
# MAGIC # TUPLES
# MAGIC ### Tuples in Python are similar to lists, but unlike lists, tuples are immutable, which means their elements cannot be changed once defined. Tuples are typically used for collections of items that should not be modified, such as coordinates, record fields, or any group of related values. Here are some examples of tuples:

# COMMAND ----------

my_tuple = (1,2,3,4,5)
print(my_tuple[0],my_tuple[-1])
print(my_tuple[:-1])
print(my_tuple[0:])

# COMMAND ----------

#Concat
tuple1 = (1, 2, 3)
tuple2 = (4, 5, 6)
concatenated_tuple = tuple1 + tuple2
print(concatenated_tuple)


# COMMAND ----------

my_tuple = (1, 2, 3)
a, b, c = my_tuple  # Unpack tuple elements into variables
print(a, b, c)

# COMMAND ----------

my_list = [1, 2, 3]
a, b, c = my_list  # Unpack list elements into variables
print(a, b, c)

# COMMAND ----------

my_tuple = (1, 2, 3)
if 100 in my_tuple:
    print("1 in tuple")
else:
    print("not")

# COMMAND ----------

#Tuple length
len(my_tuple)

# COMMAND ----------

#Count Occurrences of an Element:
my_tuple = (1, 2, 2, 3, 4, 2)
my_tuple.count(2)

# COMMAND ----------

#Count Occurrences of an Element:
my_list = [1, 2, 2, 3, 4, 2]
my_list.count(2)

# COMMAND ----------

#Find the Index of an Element:
my_tuple = (1, 2, 3, 4, 5)
index = my_tuple.index(3)
print(index)  # Outputs: 2 (index of 3 in the tuple)


# COMMAND ----------

#Find the Index of an Element:
my_tuple = (1, 2, 3, 4, 5,6,3)
index = my_tuple.index(3)
print(index)  # Outputs: 2 (index of 3 in the tuple)

# COMMAND ----------

#Check for Equality between Tuples:
tuple1 = (1, 2, 3)
tuple2 = (1, 2, 3)
are_equal = tuple1 == tuple2
print(are_equal)  # Outputs: True

# COMMAND ----------

a = 1
b = 2
c = a ==b
print(c)

# COMMAND ----------

#Check for Equality between List:
list1 = (1, 2, 3)
list2 = (1, 2, 3, 4)
are_equal = list1 == list2
print(are_equal)  # Outputs: True

# COMMAND ----------

# MAGIC %md
# MAGIC # SET Operator
# MAGIC ### Sets are unordered collections of unique elements in Python. Here are some examples of common set operations and queries:

# COMMAND ----------

#creating a set
my_set = ({2,1,3})

# COMMAND ----------

my_set
#if you observe the above while creting set, order is different and while print showing different order. No gurantee while iterating too.

# COMMAND ----------

my_set = {1,2,1,4,5,3,10}
my_set
# if you see the output, order is not same  as well as set values are chnaged to unquie elementx

# COMMAND ----------

my_set.add(6)
my_set

# COMMAND ----------

my_set.remove(6)
my_set

# COMMAND ----------

if 4 in my_set:
    print ("4 exist")

# COMMAND ----------

# UNION , DIFF, Intersection
set1 = {1, 2, 3, 4}
set2 = {3, 4, 5, 6}
union_set = set1 | set2 # union
intersection_set = set1 & set2  # Intersection
difference_set1 = set1 - set2  # Difference
difference_set2 = set2 - set1  # Difference
print(union_set)
print(intersection_set)
print(difference_set1)
print(difference_set2)

# COMMAND ----------

# we can write using
set1 = {1, 2, 3, 4, 5}
set2 = {3, 4, 5, 6, 7}

# Union of sets
union_set = set1.union(set2)

# Intersection of sets
intersection_set = set1.intersection(set2)

# Difference between sets
difference_set = set1.difference(set2)
print(union_set)
print(intersection_set)
print(difference_set)


# COMMAND ----------

len(set1)

# COMMAND ----------

for item in set1:
    print(item)

# COMMAND ----------

[item for item in set1]


# COMMAND ----------

my_set.clear()

# COMMAND ----------

my_set

# COMMAND ----------

# converting list to set
my_list = [1, 2, 2, 3, 4, 4, 5]
unique_set = set(my_list)  # Converts the list to a set, removing duplicates
print(type(unique_set))
print(union_set)

# COMMAND ----------

# check if the variable is set or not, if set return True
# Define a variable
my_variable = {1, 2, 3}

# Check if it's a list and assign the result to another variable
is_set = isinstance(my_variable, set)

# Print the result
print(is_set)  # True


# COMMAND ----------

set1 = {x ** 2 for x in range(10)}
set1

# COMMAND ----------

empty_set = {}

# COMMAND ----------

empty_set = set()

# COMMAND ----------

type(empty_set)

# COMMAND ----------

# Frozenset (Immutable Set):
frozen_set = frozenset([1, 2, 3])
frozen_set

# COMMAND ----------

#frozen_set.add(10)
#though set is mutable, we can enforce immutable by using frozenset, the error as below in the output

# COMMAND ----------

# write python functions
def fun1(x,y):
    return x+y

output = fun1(2,3)
print(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ### General Logical interview type questions in Python

# COMMAND ----------

# remove duplicates
list1 = ['a','b','a','c','b','a']
print(list1)
#option1, convert to set, it automatically remove the duplicates but it will be unorderder, lets see
setlist = set(list1)
print("Removed duplicates and in set:",setlist)



# COMMAND ----------

# write in diff way
old = ['a','b','a','c','b','a']
new = []
for item in list1:
    if item not in new:
        new.append(item)
print(new)


# COMMAND ----------

# remove the first item from the list and add to the end of the list
fruits = ['apple','banana','mango','pineapple']
idx = fruits.index('apple')
print("Index Value:",idx)
item = fruits.pop(idx)
print(item)
print(fruits)
fruits.append(item)
print(fruits)

# COMMAND ----------

# which element occurs more frequenty

a = [1,2,4,1,3,5,2,4,1,5,6,1,5,2,4,3,1,3,5,6,3,2,5,2,3,4,2,3]
max(a) # it returns only maximum value from the list
rep = max(a,key=a.count)
print("most repeated values is : ",rep)

# COMMAND ----------

# in general tuples are immutable, but below scenario we can do this way
myTuple = ([1,2],[3])
myTuple[1].append(4)
print(myTuple)

# COMMAND ----------

# remove www fromt he strings
links = [
    'www.yahoo.com',
    'www.workday.com',
    'www.gmail.com'
]
for l in links:
    print(l.lstrip("w.")) # it doesnt work
print()

for l in links:
    print(l.lstrip("www.")) # but this is missing w from workday , it works find indvidiual chars
print()
 

for l in links:
    print(l.removeprefix("www.")) # this is correct
print()



# COMMAND ----------


