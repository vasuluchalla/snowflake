from googleapiclient.discovery import build

import json
import datetime
import boto3


class FullLoadDataToS3():
    def __init__(self,Api_Key,Channel_Id) -> None:    
        self.Api_Key = Api_Key
        YouTube_Object = build('youtube', 'v3', developerKey = self.Api_Key)
        self.YouTube_Object = YouTube_Object
        self.Channel_Id = Channel_Id
        S3_Client = boto3.client("s3",region_name="eu-central-1",
                                   aws_access_key_id="xxx",
                                   aws_secret_access_key="zN/xxx"
                                   )
        self.S3_Client = S3_Client
    
    def fetch_data_from_yt(self,End_Date) -> list:
        """
        Fetches data from YouTube for the given channel IDs and date range.
        """
        
        Request = self.YouTube_Object.channels().list(
                    part='snippet,contentDetails,statistics',
                    id=self.Channel_Id)
        Response = Request.execute() 
        print(Response)
        Playlist_Id = Response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


        # Convert the end date to a datetime object
        End_Date = datetime.datetime.strptime(str(End_Date), "%Y-%m-%d %H:%M:%S.%f")

        # Initialize a flag to indicate whether there are more pages of results
        more_pages = True
        
    
        

        all_videos=[]

        # Initialize a counter to keep track of the number of videos fetched
        video_count = 0

        # Get all the video_id's from the  playlist ID of the channel for first run
        request = self.YouTube_Object.playlistItems().list(
                    part='contentDetails',
                    playlistId=Playlist_Id ,
                    maxResults = 50)
        response = request.execute()
      
  
        while more_pages==True:  
            print(str(video_count)+ "Videos Captured")  
            for i in range(len(response['items'])):
                
                video_id=response['items'][i]['contentDetails']['videoId']
                # Make a request to get the video details from the extracted video_id 
                request = self.YouTube_Object.videos().list(
                        part='snippet,statistics',
                        id=video_id)
                Response = request.execute()

                # Extract the published date of the video to check whether our published date is exceeding the end date
                Published_date = Response['items'][0]['snippet']['publishedAt']
                Published_date = datetime.datetime.strptime(str(Published_date), "%Y-%m-%dT%H:%M:%SZ")
                Published_date = Published_date.astimezone()

                End_Date = End_Date.astimezone()
                
                if Published_date < End_Date:

                    print("Fetching Data Completed")

                    print(str(len(all_videos)) ,"Videos fetched Successfully")

                    #  Set the flag to False indicate that we need to break out of the loop
                    more_pages = False

                    break
                # Write the video data to a file and append it as well to our all_videos list
              
                all_videos.append(json.dumps(Response))
                
                
            # Get the next page token from the previous response
            next_page_token = response.get('nextPageToken')
            

            
            # If there is no next page token then we set are flag as flag and break out of the Loop
            if next_page_token is None :
                

                more_pages = False
            #if there is a next page token then we get the video_ids for the next set of 50 videos
            else:
                
                
                # Increase the video count
                video_count = video_count+50

                # Make a request to get the next page of results
                request =self.YouTube_Object.playlistItems().list(
                            part='contentDetails',
                            playlistId = Playlist_Id,
                            maxResults = 50,
                            pageToken = next_page_token)
                response = request.execute()
    
        # Return the list of all videos
        print(all_videos)
       
        return all_videos    

    def dump_data_to_s3(self , Bucket_Name , Data , End_Date) ->str:
        """
        Dumps the data to S3.

        Args:
            Bucket_name: The name of the S3 bucket to save the data to.
            data: The data to save to S3.
            End_date: The date up to which the data was fetched.

        Returns:
            A string indicating whether the data was successfully dumped to S3.
        """
        try:

            Now = datetime.datetime.now()
            Year = Now.year
            Month = Now.month
            Day = Now.day
            Hour =  Now.hour
            Now_Date = f"{Year}-{Month}-{Day}-{Hour}"
            
            End_Date_Year = End_Date.year
            End_Date_Month = End_Date.month
            End_Date_Day = End_Date.day
            End_Date_Hour = End_Date.hour
            End_date_ = f"{End_Date_Year}-{End_Date_Month}-{End_Date_Day}-{End_Date_Hour}"

            # Create the final S3 key name
            File_Name = f"put_data_from{End_date_}_to_{Now_Date}"

            # Convert the data to JSON and encode it in UTF-8 
            Data = json.dumps(Data)
            Json_Object = json.loads(Data)
            Data = json.dumps(Json_Object).encode('UTF-8')
            self.S3_Client.put_object(Body = Data ,Bucket = Bucket_Name ,Key = File_Name)
            #self.S3_Client.uploads(Body = Data ,Bucket = Bucket_Name ,Key = File_Name)

            return ["Sucesss",File_Name]
        except Exception as e:
            print("Error while loading the raw_data to S3:", str(e))
            return "Failed"

    def run(self , End_Date , Bucket_Name):
        """
        Fetches data from YouTube and saves it to S3.

        Args:
            End_date: The date up to which the data should be fetched.
            Bucket_name: The name of the S3 bucket to save the data to.
        """
        try:
            
            print("Starting Script 1 ....")
            
            #Fetching Youtube Data
            Data = self.fetch_data_from_yt(End_Date)
            
            # Dump the data to S3
            Results = self.dump_data_to_s3(Bucket_Name = Bucket_Name ,Data = Data, End_Date = End_Date)
            
            print("Dump RAW_Data to S3 Results:- ",str(Results[0]))
            
            print("Ending Script 1")
            return [Results[1],len(Data)]
        except Exception as e:

            print("Error in Script 1 :" ,str(e))
        

if __name__ == "__main__":
    #Youtube API Key 
    api_key = "xxxx"

    """
    This is T-series channel id
    How to get Channel_id of any other Youtube channel ?
    open the url of the youtube channel
    Press ctrl+U
    Then Press ctrl + F then search keyword channel_id
    """
    channel_id = 'UCq-Fj5jknLsUf-MWSy4_brA'
    now = datetime.datetime.now()
    seven_day = datetime.timedelta(days=7)
    # Subtract Seven day from the current datetime
    a_week = now - seven_day
    a =FullLoadDataToS3(api_key,channel_id)
    a.fetch_data_from_yt(End_Date=a_week)
    name = 'demosnowpark'

    #Call the `FullLoadDataToS3_obj.run()` function with the date 7 days ago as the argument and bucket_name
    a.run(End_Date = a_week , Bucket_Name="demosnowpark")
