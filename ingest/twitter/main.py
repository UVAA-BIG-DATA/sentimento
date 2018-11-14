import tweepy
import json

auth = tweepy.OAuthHandler("Xu5IorVffkaHxQgnfSwGYnOP9", "ktqNLYXvZ3A3KeB0yzzjvPiJy5LSRd60BRLuNqMSbMbpjTvyPU")
auth.set_access_token("1045529018429308928-ETyP95yPXQxZcniQEa4QSVbCJgihqZ", "wW9ZWd9sictuTr75xQpet2iqDsDX0VqCPAUlWfNy3KevT")
api = tweepy.API(auth)

file_raw = open("__data__/rawTweet.txt", "a")
file_cleansed = open("__data__/cleansedData.txt", "a")


class CleansedData:

    def __init__(self, created_at="", text="", followers_count=0, timestamp_ms=0):
        self.created_at = created_at
        self.text = text
        self.followers_count = followers_count
        self.timestamp_ms = timestamp_ms


class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if status.lang == "en" and status.truncated is False:
            file_raw.write(json.dumps(status._json) + '\n')
            c = CleansedData(
                str(status.created_at),
                status.text,
                status.user.followers_count,
                status.timestamp_ms)
            file_cleansed.write(json.dumps(c.__dict__) + '\n')

    def on_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == '__main__':
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=["Amazon", "Facebook", "Netflix", "Microsoft", "Google", "Snapchat"])
