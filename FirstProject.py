import googleapiclient.discovery
from googleapiclient.errors import HttpError
import re
import mysql.connector
from datetime import datetime, timezone
#YouTube API Libraries
import googleapiclient.discovery
from googleapiclient.errors import HttpError
#MySQL Libraries
import mysql.connector
import pandas as pd
#Streamlit Libraries
import streamlit as st
from streamlit_option_menu import option_menu
import seaborn as sb
import matplotlib.pyplot as plt


api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyDMk5Ezw10dQBTaSR5yCR5zJVbfiYwzcdc"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)



#get channel details
def channel_data(channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id)
        response=request.execute()

        data = {
            "channel_id": channel_id,
            "channel_name": response['items'][0]['snippet']['title'],
            "channel_des": response['items'][0]['snippet']['description'],
            "channel_pat": response['items'][0]['snippet']['publishedAt'],
            "channel_pid": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            "channel_vc": response['items'][0]['statistics']['videoCount'],
            "channel_sub": response['items'][0]['statistics']['subscriberCount'],
            "channel_vic": response['items'][0]['statistics']['viewCount'],
        }
        return data

    #except HttpError as e:
        #print(f"HTTP error occurred: {e}")
        #return None

    #except Exception as e:
        #print(f"An error occurred: {e}")
        #return None

#channel_data('UCLbdVvreihwZRL6kwuEUYsA')
#request = youtube.playlistItems().list(
#part="snippet,contentDetails",
#playlistId="UUduIoIMfD8tT3KoU0-zBRgQ"
#)
def get_video_ids(channel_id):
    video_info = []
    request = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    )
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Now retrieve videos from the playlist
    next_page_token = None
    while True:
        playlist_request = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='contentDetails',
            maxResults=100,
            pageToken=next_page_token
        )
        playlist_response = playlist_request.execute()
        for item in playlist_response['items']:
            video_info.append(item['contentDetails']['videoId'])
        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break
    
    return video_info


#collecting video information 
def get_video_info(video_ids): 
    video_data=[]
    for video_info in (video_ids):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_info)
        response=request.execute()


        data={
            "Channel_id":response["items"][0]["snippet"]["channelId"],
            "Video_Id":response["items"][0]["id"],
            "Video_Name":response["items"][0]["snippet"]["title"],
            "Video_Description":response["items"][0]["snippet"]["description"],
            "Tags":response["items"][0].get("tags"),
            "PublishedAt":response["items"][0]["snippet"]["publishedAt"],
            "View_Count": response["items"][0]["statistics"]["viewCount"],
            "Like_Count": response["items"][0]["statistics"].get("likeCount",0),
            "Dislike_Count":response["items"][0]["statistics"].get("dislikeCount",0),
            "Favorite_Count": response["items"][0]["statistics"].get("favoriteCount",0),
            "Comment_Count": response["items"][0]["statistics"].get("commentCount",0),
            "Duration":response["items"][0]["contentDetails"].get("duration"),
            "Thumbnail":response["items"][0]["snippet"]["thumbnails"],
            "Caption_Status":response["items"][0]["contentDetails"]["caption"]
            }
        video_data.append(data)

    return video_data

#Collecting comment details 
def get_comment_info(video_ids):
    comments=[]
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50)
            response = request.execute()
            for item in response.get("items", []):
                data={
                    "Channel_id":item["snippet"]["topLevelComment"]["snippet"]["channelId"],
                    "Comment_Id":item["snippet"]["topLevelComment"]["id"],
                    "Video_Id":item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    "Comment_Text" :item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "Comment_Author":item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "Comment_PublishedAt":item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    }
                comments.append(data)
        except HttpError as e:
            if e.resp.status == 403:
                print("Comments are disabled")
            else:
                print(f"HttpError occurred: {e}")
    return comments 

def main_call(channel_id):
    # Assuming these functions return appropriate data
    channel_information = channel_data(channel_id)
    video_ids_information = get_video_ids(channel_id)
    video_info_information = get_video_info(video_ids_information)
    comment_information = get_comment_info(video_ids_information)
    
    # Creating DataFrames
    df_channel = pd.DataFrame([channel_information])
    df_video = pd.DataFrame(video_info_information)
    df_comments = pd.DataFrame(comment_information)
    
    return df_channel, df_video, df_comments

import mysql.connector
from datetime import datetime, timezone

def connect_to_mysql():
    try:
        mydb = mysql.connector.connect(
            host="localhost",  
            user="root",
            password="surya.oo7",
            database="youtube01", 
            ssl_disabled=True  
        )
        mycursor = mydb.cursor()
        print("Connected to MySQL successfully.")
        return mydb, mycursor
    except mysql.connector.Error as err:
        print("Error connecting to MySQL:", err)
        return None, None

def create_channels_table(df_channel):
    mydb, mycursor = connect_to_mysql()

    if mydb and mycursor:
            # Ensure the database is selected
            mycursor.execute("USE youtube01")  # Replace with your database name
            print("Database selected: youtube01")

            # mycursor.execute("SHOW TABLES LIKE 'Channels'")
            # table_exists = mycursor.fetchone()

            #if not table_exists:
            mycursor.execute('''CREATE TABLE IF NOT EXISTS Channels (
                                    channel_id VARCHAR(255) PRIMARY KEY,
                                    channel_name VARCHAR(255),
                                    channel_des TEXT,
                                    channel_pat TIMESTAMP,
                                    channel_pid VARCHAR(255),
                                    channel_vc INT,
                                    channel_sub INT,
                                    channel_vic INT)''')
            mydb.commit()
    print("Channels table created successfully.")

            # Inserting values into the Channel Details Table
    for index, row in df_channel.iterrows():
                # Truncate channel_des if needed
                
                
                sql = '''INSERT IGNORE INTO Channels (
                            channel_id,
                            channel_name,
                            channel_des,
                            channel_pat,
                            channel_pid,
                            channel_vc,
                            channel_sub,
                            channel_vic
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

                values = (
                    row["channel_id"],
                    row["channel_name"],
                    row["channel_des"],
                    published_date_conversion(row["channel_pat"]),
                    row["channel_pid"],
                    row["channel_vc"],
                    row["channel_sub"],
                    row["channel_vic"]
                )

                mycursor.execute(sql, values)
                mydb.commit()

    print("Data inserted into Channels table successfully.")

       
# Function to convert datetime format
def published_date_conversion(dt_str):
    dt_str = dt_str.replace("z", "")
    dt_object = datetime.fromisoformat(dt_str)
    dt_object = dt_object.replace(tzinfo=timezone.utc)
    formatted_timedate = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_timedate

import mysql.connector

def create_video_table(df_video):
    mydb, mycursor = connect_to_mysql()

    if mydb and mycursor:
            
        #try:
            
            mycursor.execute("USE youtube01")  
            print("Database selected: youtube01")

            # mycursor.execute("SHOW TABLES LIKE 'Videos'")
            # table_exists = mycursor.fetchone()

            #if not table_exists:
            mycursor.execute('''CREATE TABLE IF NOT EXISTS Videos (
                                    channel_id VARCHAR(255),
                                    Video_Id VARCHAR(255) PRIMARY KEY,
                                    Video_Name VARCHAR(250),
                                    Video_Description VARCHAR(500),
                                    PublishedAt TIMESTAMP,
                                    View_Count VARCHAR(255),
                                    Like_Count INT,
                                    Favorite_Count INT,
                                    Comment_Count INT,
                                    Duration INT
                                )''')
            mydb.commit()
    print("Video table created successfully.")

            # Inserting values into the Videos table
    for index, row in df_video.iterrows():

        # Check if Video_Id already exists in the table
        mycursor.execute("SELECT Video_Id FROM Videos WHERE Video_Id = %s", (row["Video_Id"],))
        existing_video = mycursor.fetchone()
        if existing_video:
            print(f"Skipping insert for Video_Id {row['Video_Id']} as it already exists.")
            continue

        # Truncate Video_Description if it exceeds 500 characters
        video_description = row["Video_Description"][:500] if row["Video_Description"] else None

        # Convert duration string to seconds
        duration_str = row["Duration"]
        duration_seconds = duration_to_seconds(duration_str)



        sql = '''INSERT IGNORE INTO Videos (
                    channel_id,
                    Video_Id,
                    Video_Name,
                    Video_Description,
                    PublishedAt,
                    View_Count,
                    Like_Count,
                    Favorite_Count,
                    Comment_Count,
                    Duration
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row["Channel_id"],
            row["Video_Id"],
            row["Video_Name"],
            video_description,
            published_date_conversion(row["PublishedAt"]),
            row["View_Count"],
            row["Like_Count"],
            row["Favorite_Count"],
            row["Comment_Count"],
            duration_seconds
        )

        mycursor.execute(sql, values)
        mydb.commit()

        print(f"Inserted video: {row['Video_Id']}")


# Function to convert duration string 'PT7M22S' to seconds
def duration_to_seconds(duration_str):
    try:
        if duration_str.startswith('PT'):
            hms_pattern = r'(\d+H)?(\d+M)?(\d+S)?'
            hours, minutes, seconds = re.match(hms_pattern, duration_str).groups()
            total_seconds = 0
            if hours:
                total_seconds += int(hours[:-1]) * 3600
            if minutes:
                total_seconds += int(minutes[:-1]) * 60
            if seconds:
                total_seconds += int(seconds[:-1])
            return total_seconds
        else:
            return None
    except Exception as e:
        print(f"Error converting duration {duration_str} to seconds: {e}")
        return None

# Function to convert datetime format
def published_date_conversion(dt_str):
    dt_str = dt_str.replace("z", "")
    dt_object = datetime.fromisoformat(dt_str)
    dt_object = dt_object.replace(tzinfo=timezone.utc)
    formatted_timedate = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_timedate


def create_comments_table(df_comments):
    mydb, mycursor = connect_to_mysql()

    if mydb and mycursor:
        try:
            # Ensure the database is selected
            mycursor.execute("USE youtube01")  # Replace with your database name
            print("Database selected: youtube01")

            mycursor.execute("SHOW TABLES LIKE 'Comments'")
            table_exists = mycursor.fetchone()

            if not table_exists:
                mycursor.execute('''CREATE TABLE IF NOT EXISTS Comments (
                                    Channel_id VARCHAR(255) PRIMARY KEY,
                                    Comment_Id VARCHAR(255),
                                    Video_Id VARCHAR(255),
                                    Comment_Text VARCHAR(500),
                                    Comment_Author VARCHAR(255),
                                    Comment_PublishedAt TIMESTAMP
                                    )''')
                mydb.commit()
                print("Comments table created successfully.")

            # Inserting values into the Comments Details Table
            for index, row in df_comments.iterrows():
                sql = '''INSERT IGNORE INTO Comments (
                            Channel_id,
                            Comment_Id,
                            Video_Id,
                            Comment_Text,
                            Comment_Author,
                            Comment_PublishedAt
                        ) VALUES (%s,%s,%s,%s,%s,%s)'''

                values = (
                    row["Channel_id"],
                    row["Comment_Id"],
                    row["Video_Id"],
                    row["Comment_Text"],
                    row["Comment_Author"],
                    published_date_conversion(row["Comment_PublishedAt"])
                )

                mycursor.execute(sql, values)
                mydb.commit()

            print("Data inserted into Comments table successfully.")

        except mysql.connector.Error as err:
            print("Error executing MySQL operation:", err)
        finally:
            if mycursor:
                mycursor.close()
            if mydb:
                mydb.close()
                print("MySQL connection closed.")
    else:
        print("Failed to establish MySQL connection.")
        return "Failed to establish MySQL connection."


def Tables(df_channel, df_video, df_comments):
    create_channels_table(df_channel)
    create_video_table(df_video)
    create_comments_table(df_comments)

    return "Tables Created Successfully"

def show_channels_table():
    mydb, mycursor = connect_to_mysql()
    if mydb and mycursor:
        mycursor=mydb.cursor(dictionary=True) 
        mycursor.execute("SELECT * FROM Channels")
        Channel_list=mycursor.fetchall()

        if Channel_list:
            channel_df1=st.dataframe(Channel_list)
        else:
            st.write("No records found")
            return None
        return channel_df1
    else:
        return "Failed to establish MySQL connection."
    

def show_videos_table():
    mydb,mycursor=connect_to_mysql()
    if mydb and mycursor:
        mycursor=mydb.cursor(dictionary=True)
        mycursor.execute("SELECT * FROM Videos")
        video_list=mycursor.fetchall()
        print(video_list)
        if video_list:
            video_df1=st.dataframe(video_list)
        else:
            st.write("No records found.")
            return None
        return video_df1
    else:
        return "Failed to establish MySQL connection."


def show_comments_table():
    mydb, mycursor = connect_to_mysql()
    if mydb and mycursor:
        mycursor = mydb.cursor(dictionary=True)
        mycursor.execute("SELECT * FROM Comments")
        comment_list = mycursor.fetchall()

        if comment_list:
            comment_df1 = pd.DataFrame(comment_list)
            st.dataframe(comment_df1)
        else:
            st.write("No records found.")
            return None
        return comment_df1
    else:
        return "Failed to establish MySQL connection."

    
    
def channel_exist(channel_id):
    mydb,mycursor=connect_to_mysql()
    if mydb and mycursor:
        mycursor=mydb.cursor(dictionary=True)
        mycursor.execute("SELECT EXISTS(SELECT 1 FROM Channels WHERE Channel_id=%s)", (channel_id,))
        exists = mycursor.fetchone()
        if exists:
            exists_value = list(exists.values())[0]
            print("Channel_id already exists")
            return exists_value == 1
        else:
            print("No result found")
    else:
        return "Failed to establish MySQL connection."
    


    #streamlit 
with st.sidebar:
    stream=option_menu("",["Home","DataHarvesting","DataWarehousing","QueryData"])
    index=1  

if stream=="Home":
    st.title('This is a title')
    st.title("YOUTUBE DATA HARVESTING AND DATA WAREHOUSING")
    st.subheader(":red[Domain:]  Social Media")
    st.subheader(":red[Overview:]") 
    st.markdown('''Build a simple dashboard or UI using Streamlit and 
                        retrieve YouTube channel data with the help of the YouTube API.
                        Store the data in an SQL database (data warehousing),
                        enable queries in SQL, and finally display in Streamlit''')



elif stream=="DataHarvesting":
    st.markdown("""<h1 style='text-align: center; color:red;'>DataHarvesting</h1>""",unsafe_allow_html=True)

    channel_id=st.text_input("Enter the Channel Id")

    mydb,mycursor=connect_to_mysql()
    if st.button("Collect and Store Data"):
        try:
            channel_str = channel_data(channel_id)

            # Display channel data
            st.subheader('Channel Information')
            df_channel = pd.DataFrame([channel_str])
            st.write(df_channel)

            # Save data to MySQL
            create_channels_table(df_channel)

        except Exception as e:
            st.error(f"Error fetching data: {e}")

        if mydb and mycursor:
            print(channel_exist(channel_id))
            if (channel_exist(channel_id)):
                st.success("Channel details already exists.")
            else:
                df_channel,df_video,df_comment=main_call(channel_id)
                Tables(df_channel, df_video, df_comment)
                st.success("Data Collected and Stored Succesfully")
                #mydb,mycursor.close()
        else:
            st.error("Failed to connect MySQL Database")


#DataWarehousing Zone
elif stream=="DataWarehousing":
    mydb,mycursor=connect_to_mysql()
    st.markdown("""<h1 style='text-align: center; color:red;'>DataWarehousing</h1>""",unsafe_allow_html=True)
    def get_all_channels():
        mycursor=mydb.cursor()
        mycursor.execute("SELECT Channel_id FROM Channels")
        all_channels_list=mycursor.fetchall()
        return [channel[0] for channel in all_channels_list]
    def display_tables(channel_id):
        pass
    if mydb and mycursor:
        all_channels=get_all_channels()
        show_table=st.selectbox("CHOOSE THE TABLE FOR VIEW", ("Channels_Table","Videos_Table","Comments_Table"))

        if show_table=="Channels_Table":
            show_channels_table()
        if show_table=="Videos_Table":
            show_videos_table()
        if show_table=="Comments_Table":
            show_comments_table()

    #converting duration format
def convert_duration(duration):
    hours = int(duration // 3600)
    minutes = int((duration % 3600) // 60)
    seconds = int(duration % 60)
    return '{:02}:{:02}:{:02}'.format(hours, minutes, seconds)


   
# Query Zone
if stream == "QueryData":
    st.markdown("""<h1 style='text-align: center; color:red;'>QueryData</h1>""", unsafe_allow_html=True)
    mydb, mycursor = connect_to_mysql()

    # Creating queries using selectbox
    Question = st.selectbox("Select your question", (
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos, and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
    ))

    if mydb and mycursor:
        if Question.startswith("1."):
            query1 = '''
                SELECT Video_name, channel_name
                FROM Channels
                JOIN videos ON Channels.channel_id = videos.channel_id ORDER BY channel_name
            '''
            try:
                mycursor.execute(query1)
                q = mycursor.fetchall()
                df = pd.DataFrame(q, columns=["video_name", "channel_name"]).reset_index(drop=True)
                df.index += 1
                st.dataframe(df)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 1: {err}")

        elif Question.startswith("2."):
            query2 = '''
                SELECT Channel_name, COUNT(Video_Id) AS No_of_Videos
                FROM Videos
                JOIN channels ON channels.channel_id = videos.Channel_id
                GROUP BY Channel_name
            '''
            try:
                mycursor.execute(query2)
                q1 = mycursor.fetchall()
                df1 = pd.DataFrame(q1, columns=["Channel Name", "No of Videos"]).reset_index(drop=True)
                df1.index += 1
                st.dataframe(df1)
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df1, x="Channel Name", y="No of Videos", ax=ax)
                ax.set_xlabel("Channel Name")
                ax.set_ylabel("No of Videos")
                ax.set_title("Total Number of Videos By Channel")
                ax.set_xticklabels(df1["Channel Name"], rotation=45, ha="right")
                st.pyplot(fig)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 2: {err}")



# Assuming you have imported required libraries and set up MySQL connection properly

        elif Question.startswith("3."):
            query3 = '''
                SELECT Channel_Name, Video_Name, View_Count
                FROM Videos 
                JOIN Channels ON Channels.Channel_id = Videos.Channel_id
                ORDER BY Videos.View_count DESC LIMIT 10
            '''
            try:
                mycursor.execute(query3)
                q2 = mycursor.fetchall()
                df2 = pd.DataFrame(q2, columns=["Channel Name", "Video Name", "Total Views"]).reset_index(drop=True)
                
                # Convert "Total Views" to numeric type (if not already)
                df2["Total Views"] = pd.to_numeric(df2["Total Views"], errors="coerce")
                
                st.dataframe(df2)
                
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df2, x="Video Name", y="Total Views", ax=ax)
                ax.set_xlabel("Video Name")
                ax.set_ylabel("Total Views")
                ax.set_title("Top 10 Most Viewed Videos")
                ax.set_xticklabels(df2["Video Name"], rotation=90)
                ax.get_yaxis().get_major_formatter().set_scientific(False)
                
                st.pyplot(fig)

            except mysql.connector.Error as err:
                st.error(f"Error executing query 3: {err}")

            except Exception as e:
                st.error(f"Error: {e}")


        elif Question.startswith("4."):
            query4 = '''
                SELECT Comment_count, Video_name 
                FROM Videos 
                ORDER BY Comment_count DESC LIMIT 10
            '''
            try:
                mycursor.execute(query4)
                q3 = mycursor.fetchall()
                df3 = pd.DataFrame(q3, columns=["Comment Count", "Video Name"]).reset_index(drop=True)
                df3.index += 1
                st.dataframe(df3)
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df3, x="Video Name", y="Comment Count", ax=ax)
                ax.set_xlabel("Video Name")
                ax.set_ylabel("Comment Count")
                ax.set_title("Total No of Comments")
                ax.set_xticklabels(df3["Video Name"], rotation=90)
                st.pyplot(fig)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 4: {err}")

        elif Question.startswith("5."):
            query5 = '''
                SELECT Channel_Name, Video_Name, Like_Count 
                FROM Videos 
                JOIN Channels ON Channels.Channel_Id = Videos.Channel_Id 
                ORDER BY Like_Count DESC LIMIT 10
            '''
            try:
                mycursor.execute(query5)
                q4 = mycursor.fetchall()
                df4 = pd.DataFrame(q4, columns=["Channel Name", "Video Name", "No of Likes"]).reset_index(drop=True)
                df4.index += 1
                st.dataframe(df4)
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df4, x="Video Name", y="No of Likes", ax=ax)
                ax.set_xlabel("Video Name")
                ax.set_ylabel("No of Likes")
                ax.set_title("Highest Number of Likes")
                ax.set_xticklabels(df4["Video Name"], rotation=90)
                ax.get_yaxis().get_major_formatter().set_scientific(False)
                st.pyplot(fig)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 5: {err}")

        elif Question.startswith("6."):
            query6 = '''
                SELECT Video_Name, Like_Count, Dislike_Count 
                FROM Videos 
            '''
            try:
                mycursor.execute(query6)
                q5 = mycursor.fetchall()
                df5 = pd.DataFrame(q5, columns=["Video Name", "No of Like_Count", "No of Dislike_Count"]).reset_index(drop=True)
                df5.index += 1
                st.dataframe(df5)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 6: {err}")


        elif Question.startswith("7."):
            query7 = '''
                SELECT Channel_Name, channel_vc 
                FROM Channels 
            '''
            try:
                mycursor.execute(query7)
                q6 = mycursor.fetchall()
                df6 = pd.DataFrame(q6, columns=["Channel Name", "Total Views"]).reset_index(drop=True)
                df6.index += 1
                st.dataframe(df6)
                df6["Total Views in k"] = df6["Total Views"] / 1000
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df6, x="Channel Name", y="Total Views in k", ax=ax)
                ax.set_xlabel("Channel Name")
                ax.set_ylabel("Total Views")
                ax.set_title("Total Number of Views")
                ax.set_xticklabels(df6["Channel Name"], rotation=90)
                st.pyplot(fig)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 7: {err}")

        elif Question.startswith("8."):
            query8 = '''
                SELECT Channel_Name, PublishedAt 
                FROM Videos 
                JOIN Channels ON Channels.channel_id = Videos.channel_id 
                WHERE YEAR(PublishedAt) = 2022
            '''
            try:
                mycursor.execute(query8)
                q7 = mycursor.fetchall()
                df7 = pd.DataFrame(q7, columns=["Channel Name", "Published Year(2022)"]).reset_index(drop=True)
                df7.index += 1
                st.dataframe(df7)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 8: {err}")

                    # Define the conversion function
            def convert_duration(duration_in_seconds):
                return duration_in_seconds / 60  # Convert seconds to minutes

            def convert_duration(seconds):
                return seconds / 60  # Convert seconds to minutes
            



        elif Question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query9 = '''
                SELECT Channel_Name,AVG(Duration) FROM Videos join channels 
                on channels.channel_id=videos.channel_id 
                group by Channel_Name
            '''
            
            mycursor.execute(query9)
            q8= mycursor.fetchall()
                
                # Process query results and format for DataFrame
            results_formatted = [(channel_name, avg_duration) for channel_name, avg_duration in q8]
                
            df8 = pd.DataFrame(results_formatted, columns=["channel name", "Average Duration"]).reset_index(drop=True)
            df8.index += 1
                
                # Sort DataFrame by Average Duration
            df8 = df8.sort_values(by="Average Duration")
                
                # Display DataFrame in Streamlit
            st.dataframe(df8)
                
                # Plotting
            fig, ax = plt.subplots(figsize=(10, 6))
            sb.barplot(data=df8, x="Channel Name", y="Average Duration", ax=ax)
            ax.set_xlabel("Channel Name")
            ax.set_ylabel("Average Duration")
            ax.set_title("Average Duration of Channels")
            ax.set_xticklabels(df8["Channel Name"], rotation=90)
            st.pyplot(fig)
            
            #except mysql.connector.Error as err:
                #st.error(f"Error executing query: {err}")


        elif Question.startswith("10."):
            query10 = '''
                SELECT Channel_Name, Video_Name, Comment_count 
                FROM Videos 
                JOIN Channels ON Channels.channel_id = Videos.channel_id 
                ORDER BY Videos.Comment_Count DESC LIMIT 10
            '''
            try:
                mycursor.execute(query10)
                q9 = mycursor.fetchall()
                df9 = pd.DataFrame(q9, columns=["Channel Name", "Video Name", "No of Comment"]).reset_index(drop=True)
                df9.index += 1
                st.dataframe(df9)
                fig, ax = plt.subplots(figsize=(10, 6))
                sb.barplot(data=df9, x="Video Name", y="No of Comment", ax=ax)
                ax.set_xlabel("Video Name")
                ax.set_ylabel("No of Comment")
                ax.set_title("Highest Number of Comments")
                ax.set_xticklabels(df9["Video Name"], rotation=90)
                ax.get_yaxis().get_major_formatter().set_scientific(False)
                st.pyplot(fig)
            except mysql.connector.Error as err:
                st.error(f"Error executing query 10: {err}")