import csv

with open("trips.csv", "rb") as csvfile:
	with open("trips1.csv", 'wb') as outfile:
		thereader = csv.reader(csvfile, delimiter=',')
		thewriter = csv.writer(outfile, delimiter=',')
		for row in thereader:
			thewriter.writerow(row + ["Point(11.1 22.2)"])
