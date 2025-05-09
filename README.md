# REST-API-Showcase
Showcase of REST APIs that make use of MySQL through Google Cloud Services. I also uploaded a Docker image to the Google Artifact Registry to allow the app to be run through Google Compute Engine.


# Goals
The main goal for this project was to build REST APIs and allow for CRUD operations through MySQL. A secondary goal was to learn about Docker, containerization, and image creation. The final goal for this project was to upload my Docker image to Google Cloud so that I could run the project through Google's Compute Engine.


# Step One
My first step was to work on the REST APIs. I only had two tables to work with in my database: businesses and reviews. I built all four CRUD operations for both businesses and reviews. I also added a couple of different get functions, like get all businesses belonging to a specific owner, or get all reviews made by a specific user. For this step, I could have improved things by taking advantage of more database features, like foreign keys and constraints, to make the endpoints easier to work with. Another thing I could have done better during this was to make more functions to have less redundancy.


# Step Two
The second step was to make a Dockerfile to create an image. At this point, I had never used Docker, so videos, documentation, and articles were helpful during this step. After understanding Docker a bit more, I was able to make a Dockerfile that recreated my environment. One thing I could improve on in this step is getting a deeper understanding of Docker and getting more practice with it.


# Step Three
My final step was getting my project running through the Google Compute Engine service. I started by uploading my image to the Google Artifact Registry. After doing so, I created a VM with the Google Compute Engine service and configured it to use the uploaded image file to create a container. After the VM started up, I also added a firewall rule to allow different traffic in so I could reach the endpoints I had made. 


# Closing thoughts
Overall, this project was a lot of fun, and I'm glad I got to brush up on some SQL and started learning Docker. As previously mentioned, to improvements on this project are to add foreign keys and constraints to the tables, clean up redundant code by adding more functions, specifically error handling, and further my experience and learning of Docker.
