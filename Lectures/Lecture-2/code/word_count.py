# Open the file in read mode 
lines = open("..\data\sample.txt", "r") 

# Create an empty dictionary 
word_counts = {} 

# Loop through each line of the file 
for line in lines: 
	# Split the line into words 
	words = line.split(" ") 

	# Iterate over each word in line 
	for word in words: 
		# Remove punctuation and special characters
		word = ''.join(char for char in word if char.isalnum())
		
		# Check if the word is already in dictionary 
		if word: 
			# Increment count of word by 1 
			word_counts[word] = word_counts.get(word, 0) + 1

# Sort the word counts by count in descending order
sorted_word_counts = sorted(word_counts.items(), key=lambda word: word[1], reverse=True)

# Display the top 20 words
print("Top 20 Word Counts:")
for word, count in sorted_word_counts[:20]:
	print(f"{word}: {count}") 

# Display the bottom 20 words
print("Bottom 20 Word Counts:")
for word, count in sorted_word_counts[-20:][::-1]:
	print(f"{word}: {count}") 
