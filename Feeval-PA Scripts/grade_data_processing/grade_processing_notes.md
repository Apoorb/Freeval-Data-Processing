1. Pull the min/max range for each segment.
    - For segments with length > 1 mile, it would be based on the segment.
	- For segments < 1mile (e.g. merge and diverge segments), we would look 0.5 miles upstream and 0.5 miles downstream.
2. If less than 0.5% difference in grade, compute weighted average
	- If that results in too many outliers, let's look for 1% min/max range allowable error.
3. For classifying into level, rolling, 'specific grade', and vertical curves. we have the following definitions:
	1. 'specific grade': Freeway and multi-lane highway segments longer than 0.5 mi with grades between 2% and 3% or longer than 0.25 mi with grades of 3% or greater should
be considered as separate segments.~HCM 6th Ed, Ch 12
        - Only applicable on freeval segments greater than 0.25 miles and range
		- Only look at range < 0.5 % (1 %, if 0.5 % is too strict)
		- Flag these segments to be broken---would be hard to break if say it is a weaving segment
		- Will use the weighted average grades of individual broken segments
		- Fill PCE value. I am planning on using 30% SUTs and 70% TTs table (Exhibit 12-26)
	2. vertical curves: range grade > 0.5 % (1 %, if 0.5 % is too strict)
		- Flag these. Need to break it in 2 on more sub-segments.
	3. level: anything that doesn't fall under 'specific grade' AND vertical curves AND weighted average grade <= 2%
rolling: Anything that doesn't fall under level AND 'specific grade' ANDe.