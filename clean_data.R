#Load packages
library("jsonlite")
library("rjson")
library("dplyr")
library("stringr")
library("usmap")
library("ggplot2")
library("gridExtra")
library("grid")
library("tidyverse")
library("texreg")
library("readr")
library("tidytext")
library("quanteda")
library("glmnet")
library("yardstick")
library("textcat")

setwd("/Users/mjuul/Documents/cbs_ha_it/5_semester/dataScience/yelp")


# Magnus 
# Create a sub folder with following structure, from the r file location, place the json files from keggle in the "raw" directory
# /data/raw
# Link to data: https://www.kaggle.com/yelp-dataset/yelp-dataset
#Set working directory
loadRawData <- function () {

#Load US states
 usStates <- read.csv("./data/raw/usStates.csv")

#Load JSON data
users <- jsonlite::stream_in(file("./data/raw/yelp_academic_dataset_user.json"),pagesize = 100000) # reads line by line, pagesize size is given to break records into chunks

business <- jsonlite::stream_in(file("./data/raw/yelp_academic_dataset_business.json"),pagesize = 100000) # reads line by line, pagesize size is given to break records into chunks
business_flat <- jsonlite::flatten(business) # convert into more nested columns

dat <- readLines("./data/raw/yelp_academic_dataset_review.json")
reviews <- jsonlite::stream_in(textConnection(dat))


#Clean data
business_filtered <- dplyr::filter(business_flat, grepl("Restaurants", categories),  # only keeps "Restuarant businesses  
                                   state %in% usStates$Code) %>% #Only keep US business
  select(business_id, name, address, city, state, postal_code, longitude, latitude, stars, review_count, is_open, categories, attributes.RestaurantsPriceRange2)

reviews_filtered <- reviews %>%
  filter(business_id %in% business_filtered$business_id)
reviews_filtered$review_word_count <- str_count(reviews_filtered$text, " ") + 1

users_filtered <- users %>%
  filter(user_id %in% reviews_filtered$user_id) %>%
  group_by(user_id)
users_filtered$friends_count <- str_count(users_filtered$friends, ",") + 1

reviews_filtered <-  reviews_filtered %>%
  filter(user_id %in% users_filtered$user_id)

business_filtered <- business_filtered %>%
  filter(business_id %in% reviews_filtered$business_id)

write.csv(business_filtered, "./data/YelpBusinessesFiltered.csv", row.names = FALSE)
write.csv(reviews_filtered, "./data/YelpReviewsFiltered.csv", row.names = FALSE)
write.csv(users_filtered, "./data/YelpUsersFiltered.csv", row.names = FALSE)

return(invisible())
}

USMapAnalysis <- function (){
  
  #Load state population
  state_population <- read.csv("./data/raw/state_population.csv") %>%
    rename(population = "X2018.Population") 
  state_population <- merge(x=state_population,y=read.csv("./data/raw/usStates.csv"),by="State",all.x=TRUE) %>%
    rename(state_long = "State",
           state = "Code")

  #merge review and bussiess data
  reviewsStateInfo <- merge(x=csv_review,y=csv_business,by="business_id",all.x=TRUE) %>%
    subset(select = c(review_id,business_id, user_id, stars.x , review_word_count, state, date))

  #Calculate factors
  usStatesDf = reviewsStateInfo %>% group_by(state) %>%
    summarise(review_count = n_distinct(review_id),
              avg_rating = mean(stars.x),
              user_count = n_distinct(user_id),
              business_count = n_distinct(business_id))
  
  #Merge population and state data
  usStatesDf <- merge(x=usStatesDf, y=state_population, by="state", all.x = TRUE) %>%
    subset(select = -c(Abbrev )) %>%
    mutate(reviews_from_each_user = review_count / user_count,
           reviews_pr_store =  review_count / business_count,
           percentage_of_population = population / sum(population),
           percentage_of_reviews = review_count / sum(review_count),
           population_reviews_percentage_difference = review_count / population,
           state_count = 1)
  
  usStatesDfFiltered <- usStatesDf %>%
    filter(business_count > 10)
  
  

#Reviews pr yeqr analysis:
  reviewsByYear <- reviewsStateInfo %>%
    filter(state %in% usStatesDfFiltered$state) %>%
    mutate(year = substr(date, start = 1, stop = 4),
           review_count = 1) %>%
    subset(select = c(state,year,review_count)) %>%
    group_by(year, state) %>% 
    summarise(review_count = sum(review_count))

  reviewsByYear %>%
    ggplot(aes(x=as.numeric(year), y=review_count)) +
    geom_line(aes(colour=state, linetype=state)) +
    labs(x = "Year", y = "Reviews written") 
    
  
  #Benford's law
  bamfordsLaw <- csv_review %>%
    subset(select =c(review_word_count)) %>%
    mutate(n = 1,
           bamford = substr(review_word_count, start =1, stop =1)) %>%
    subset(select =c(bamford, n)) %>%
    group_by(bamford) %>%
    summarise(n  = sum(n ))
  
  bamfordsLaw %>%
    ggplot(aes(x=bamford, y=n/sum(n))) +
    geom_bar(stat='identity')
  
  
  #Bussiness pr state analysis
  gplot1 <- plot_usmap(data = usStatesDf, values = "business_count", color = "black") + 
    scale_fill_continuous(
      low = "red", high = "green", name = "Business pr state", label = scales::comma
    ) + theme(legend.position = "right") + 
    theme(legend.position="none")
  
  
  gplot2 <- usStatesDf %>%
    ggplot(aes(x = reorder(state_long, business_count), y = business_count, fill=business_count)) +
    scale_fill_gradient(low = "red", high = "green") +
    geom_bar(stat='identity') +
    theme_minimal() + 
    labs(x = "", y = "Business Count pr state") +
    coord_flip() + 
    labs(fill='Business pr state') 
  
  grid.arrange(gplot1, gplot2, ncol=2,
               top = textGrob("Restaurant business in each state",gp=gpar(fontsize=20,font=3)))  
  
  
  #avg_rating
gplot1 <- usStatesDfFiltered %>%
  ggplot(aes(x = reorder(state, avg_rating), y = avg_rating)) +
  geom_bar(stat='identity', color="blue") +
  theme_minimal() +
  coord_flip() 


gplot2 <- plot_usmap(data = usStatesDfFiltered, values = "avg_rating", color = "blue", include=c(usStatesDfFiltered$state)) + 
  scale_fill_continuous(
    low = "white", high = "blue", name = "Average Rating", label = scales::comma
  ) + theme(legend.position="none")


grid.arrange(gplot1, gplot2, ncol=2,
             top = textGrob("Restaurant business in each state",gp=gpar(fontsize=20,font=3)))  


#reviews pr store analysis
gplot1 <- plot_usmap(data = usStatesDfFiltered, values = "reviews_pr_store", color = "blue", include=c(usStatesDfFiltered$state)) + 
  scale_fill_continuous(
    low = "red", high = "green", name = "reviews_pr_store", label = scales::comma) +  
  theme(legend.position="none")

gplot2 <- usStatesDfFiltered %>%
  ggplot(aes(x = reorder(state_long, reviews_pr_store), y = reviews_pr_store, fill=reviews_pr_store)) +
  scale_fill_gradient(low = "red", high = "green") +
  geom_bar(stat='identity') +
  theme_minimal() + 
  labs(x = "", y = "Average reviews pr store") +
  coord_flip() + 
  labs(fill='Average reviews pr store') 


grid.arrange(gplot1, gplot2, ncol=2,
             top = textGrob("Average reviews pr business",gp=gpar(fontsize=20,font=3)))  

#reviews pr User
gplot1 <- plot_usmap(data = usStatesDfFiltered, values = "reviews_pr_user", color = "blue", include=c(usStatesDfFiltered$state)) + 
  scale_fill_continuous(
    low = "red", high = "green", name = "reviews_pr_user", label = scales::comma) +  
  theme(legend.position="none")

gplot2 <- usStatesDfFiltered %>%
  ggplot(aes(x = reorder(state_long, reviews_pr_user), y = reviews_pr_user, fill=reviews_pr_user)) +
  scale_fill_gradient(low = "red", high = "green") +
  geom_bar(stat='identity') +
  theme_minimal() + 
  labs(x = "", y = "Average reviews pr store") +
  coord_flip() + 
  labs(fill='Average reviews pr store') 

grid.arrange(gplot1, gplot2, ncol=2,
             top = textGrob("Average reviews pr User",gp=gpar(fontsize=20,font=3)))  
  
  return(invisible())
}

CorrelationAnalysis <- function(){
  #Random number generation
  set.seed(123)
  #Making the dataset smaller with taking a 10% fractile of the dataset.
  review <- sample_frac(csv_review, 0.1) 
  #Overview over the structure, dataformats and examples of what is included in the varibles
  str(review)
  help(str)
  #summary and statistic metrics 
  summary(review)
  summary(csv_review)
  
  ## The correlation between stars and review word count.
  cor(review$stars, review$review_word_count)
  #Plotting the correlation - seen as a categorial predictor -  ordinal variable 
  plot(review$stars, review$review_word_count, main="Scatterplot")
  
  
  ## Finding correlations for business - A better corelation but not that good = 0,6 
  cor(csv_business$stars, csv_business$review_count)
  plot(csv_business$stars, csv_business$review_count, main="Scatterplot")
  
  ## Corelation bewteen users and other numeric varibles
  #Finding a correlation between the length of the review and the stars. 
  set.seed(123)
  users <- sample_frac(csv_user, 0.1) 
  str(users)
  
  #summary - to see if the fractile dataset is representative compared to the real dataset
  summary(users)
  summary(csv_user)
  
  cor(users$review_count, users$useful) #There is a okay correlation (0,522)
  #shows linearity - The relationship between the independent and dependent variable is linear
  
  #LM linar regression - x and y lim to the axsis also. Another plot
  ggplot(users, aes(x=review_count, y=useful)) + 
    geom_point(alpha = 0.3) +
    stat_smooth(method = "lm", col = "red")+
    xlim(0,4000)+
    ylim(0,10000)+
    labs(x = "Review count Per user", y = "amount of 'usefull' statements",
         title = "Visualization of correlation ",
         caption = "Source: Yelp dataset/ Kaggle")
  
  #to make correlation matrix - we need to reducing non-nomeric varibles 
  users.numeric <- users[,sapply(users, is.numeric)]
  
  #The correlation matrix - to find possible predictors for LM model. 
  round(cor(users.numeric),
        digits = 2 # rounded the correlation coef to 2 decimals
  )
  
  #pairvise corelation matrix 
  pairs(~ review_count + useful + fans + friends_count , data = users.numeric )
  
  #building  linear models around these correlations
  m1 <- lm(review_count ~  useful , data = users)
  m2 <- lm(review_count ~  fans , data = users)
  m3 <- lm(review_count ~  friends_count , data = users) 
  m4 <- lm(review_count ~  useful + fans , data = users)
  m5 <- lm(review_count ~  useful + fans + friends_count , data = users) 
  m6 <- lm(review_count ~  useful *fans , data = users)
  
  #Summary test
  summary (m5)
  
  #Statistical model output - printing the  models side by side for comparison 
  texreg::screenreg(list(m1, m2, m3, m4 ,m5,m6))
  
  #Seeing the fitted values and residual for review count using model1 
  resiudals <- data.frame (y = review_count[1:5, "review_count"],
                           y_hat = fitted(m1)[1:5],
                           e_hat = resid(m1)[1:5], data= users.numeric)
  
  #Creating new dataframe with all combinations of varibles limited by 
  #a sequence margin, categorical division or mean
  plotting.data<-expand.grid(
    useful = c(1, mean(users.numeric$useful), max(users.numeric$useful)),
    review_count = seq(1, max(users.numeric$review_count), by=10),
    friends_count =mean(users.numeric$friends_count) ,
    fans = mean(users.numeric$fans))
  
  #Save predicted y values into our dataset
  plotting.data$predicted.y <- predict.lm(m4, newdata=plotting.data)
  
  #The first three rows from plotting.data
  head(plotting.data, 3)
  
  #Change the friends_count varible into a factor - so that we can plot the 
  #interaction between review_count, fans and firends_count at each of the 
  #three levels of useful we chose.
  plotting.data$useful <- as.factor(plotting.data$useful)
  
  ggplot(users.numeric, aes(x = review_count, y = useful)) +
    geom_point() +
    geom_line(data=plotting.data, aes(x=review_count, y=predicted.y, color=useful), size=1.25) +
    xlim(0,4000)+
    ylim(0,10000)
  
  ##Diagnostic tests
  #Save predicted y values into our dataset
  users.numeric$predicted.y <- predict.lm(m4, newdata=users.numeric)
  reg_fortify <- fortify(users.numeric)
  
  head(fortify(users.numeric))
  
  ggplot(reg_fortify, aes(.fitted, .resid)) +
    geom_point() +
    geom_hline(yintercept = 0) +
    geom_smooth(se = FALSE) +
    labs(title = "Residuals vs. Fitted",
         y = "Residuals",
         x = "Fitted values")
}

Regression <- function(){
  reviews <- read.csv("./data/YelpreviewsFiltered.csv")
  
  # Create a language variable using the "textcat" package
  
  textcat(reviews[50:80,]$text)
  reviews$language <- textcat(reviews$text)
  
  # Filter out non-english reviews
  
  reviews <- filter(reviews, language == "english")
  
  # Save as csv
  
  write.table(reviews, file = "reviews.csv")
  
  # Create sentiment variable
  
  reviews <- filter(reviews, stars != 3)
  reviews$positive <- as.factor(reviews$stars > 3)
  summary(reviews$positive)
  
  # Create document feature matrix
  
  corp <- corpus(reviews$text,
                 docvars = data.frame(positive = reviews$positive,
                                      id = reviews$review_id,
                                      stars = reviews$stars))
  dfm <- corp %>%
    tokens() %>%
    dfm()
  docvars(dfm) <- data.frame(positive = reviews$positive,
                             id = reviews$review_id,
                             stars = reviews$stars)
  dfm <- dfm_trim(dfm, min_docfreq = 2)
  
  # Splitting the data
  
  uid <- unique(reviews$review_id)
  training <- sample(1:length(uid), floor(0.8 * length(uid)))
  test <- (1:length(uid)) [1:length(uid) %in% training == FALSE]
  train_id <- uid[training]
  test_id <- uid[test]
  train_dfm <- dfm_subset(dfm, id %in% train_id)
  test_dfm <- dfm_subset(dfm, id %in% test_id)
  
  # Ridge and lasso
  
  ridge <- cv.glmnet(train_dfm, docvars(train_dfm, "positive"),
                     family = "binomial", alpha = 0,
                     nfolds = 5, intercept = TRUE,
                     type.measure = "class")
  lasso <- cv.glmnet(train_dfm, docvars(train_dfm, "positive"),
                     family = "binomial", alpha = 1, nfolds = 5,
                     intercept = TRUE, type.measure = "class")
  
  # Predictions
  
  preds_ridge <- predict(ridge, test_dfm, type = "class")
  table(preds_ridge, docvars(test_dfm, "positive"))
  
  preds_lasso <- predict(lasso, test_dfm, type = "class")
  table(preds_lasso, docvars(test_dfm, "positive"))
  
  # Comparisons and evaluation
  
  results <- data.frame(obs = factor(docvars(test_dfm, "positive")),
                        p_ridge = factor(preds_ridge),
                        p_lasso = factor(preds_lasso))
  accuracy(results, obs, p_ridge)
  accuracy(results, obs, p_lasso)
  
  precision(results, obs, p_ridge, event_level = "second")
  precision(results, obs, p_lasso, event_level = "second")
  
  precision(results, obs, p_ridge, event_level = "first")
  precision(results, obs, p_lasso, event_level = "first")
  
  recall(results, obs, p_ridge, event_level = "second")
  recall(results, obs, p_lasso, event_level = "second")
  
  recall(results, obs, p_ridge, event_level = "first")
  recall(results, obs, p_lasso, event_level = "first")
  
  # Lasso results
  
  best.lambda.lasso <- which(lasso$lambda == lasso$lambda.1se)
  
  beta.lasso <- lasso$glmnet.fit$beta[, best.lambda.lasso]
  
  df.lasso <- data.frame(coef = as.numeric(beta.lasso),
                         word = names(beta.lasso),
                         stringsAsFactors = FALSE)
  
  arrange(df.lasso, desc(coef))
  arrange(df.lasso, coef)
  
  # Ridge results
  
  best.lambda.ridge <- which(ridge$lambda == ridge$lambda.1se)
  
  beta.ridge <- ridge$glmnet.fit$beta[, best.lambda.ridge]
  
  df.ridge <- data.frame(coef = as.numeric(beta.ridge),
                         word = names(beta.ridge),
                         stringsAsFactors = FALSE)
  
  arrange(df.ridge, desc(coef)) %>%
    head()
  arrange(df.ridge, coef) %>%
    head()
}

#Uncomment row below to load data from raw files (Can take a long time to run)
loadRawData() (Magnus)
#Load filtered data
csv_business <- read.csv("./data/YelpBusinessesFiltered.csv")
csv_review <- read.csv("./data/YelpReviewsFiltered.csv")
csv_user <- read.csv("./data/YelpUsersFiltered.csv")


#Run US maps analysis (Magnus)
USMapAnalysis()

#Correlation analysis (Barbara)
CorrelationAnalysis()

#Reggresion (Marco)
Regression()