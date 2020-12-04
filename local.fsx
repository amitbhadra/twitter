open System.Threading
#load @"./Constants.fsx"
#load @"./MessageTypes.fsx"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: Akka.Remote" 
#r "nuget: FSharp.Json"


open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration
open MessageTypes
open Constants.Constants
open System.Text.RegularExpressions
open FSharp.Json

let config =
    Configuration.parse
        @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.dot-netty.tcp {
                hostname = ""127.0.0.1""
                port = 9001
            log-dead-letters-during-shutdown = off
            log-dead-letters = off
            }
        }"
        
let mutable allTweetsSent = 0
let mutable totalUsers = 0
 
let system = System.create "Twitter" config

let MyUserActor (actorNameVal:string) (actorId:int) (operation:string) (mailbox : Actor<_>) = 

    let selfName = actorNameVal
    let selfId = actorId
    let modeOfOperation = operation
    let mutable selfTweets =  Array.create 0 ""
    let mutable receivedTweets = Array.empty
    let selfStopwatch = System.Diagnostics.Stopwatch()
    let mutable oldTime = 0.0
    let mutable alive = true

    let rec loop() = actor {

        if float(selfStopwatch.Elapsed.TotalSeconds) - oldTime > 0.5 && not alive then
            oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)
            alive <- true
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "GoOnline"}
            let json = Json.serialize data
            destinationRef <! json
            Console.WriteLine("[{0}]==>Setting {1} online",selfStopwatch.Elapsed.TotalSeconds,  selfId)

        let! json = mailbox.Receive()
        let message = Json.deserialize<TweetApiMessage> json
        let sender = mailbox.Sender()
        let operation = message.Operation

        match operation with
        | "Register" ->
            selfStopwatch.Start()
            Console.WriteLine("[{0}]==>Registered: {1}", selfStopwatch.Elapsed.TotalSeconds, selfName)

        | "StartUser" ->
            if alive then
                let mutable actionId = random.Next(actions.Length)
                if actions.[actionId] = "tweet" then
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "TweetInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "retweet" then
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "RetweetInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "subscribe" && modeOfOperation <> "zipf" then
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "SubscribeInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "query" then
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "QueryInit"}
                    let json = Json.serialize data
                    destinationRef <! json
            let mutable timeNow = float(selfStopwatch.Elapsed.TotalMilliseconds)
            while float(selfStopwatch.Elapsed.TotalMilliseconds) - timeNow < 10.0 do
                0|> ignore
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
            let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
            let json = Json.serialize data
            destinationRef <! json


        | "GoOffline"  ->
            if alive then
                alive <- false
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)

        | "TweetInit" ->
            let mutable mentionUserBoolean = random.Next(2)
            if mentionUserBoolean = 1 then
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = ""; Operation = "GetNumNodes"}
                let json = Json.serialize data
                destinationRef <! json
            else
                let mutable randomMessageId = random.Next(tweets.Length)
                let mutable randomHashtagId = random.Next(hashtags.Length*2)
                if randomMessageId < tweets.Length then
                    let mutable tweetString = tweets.[randomMessageId]
                    if randomHashtagId < hashtags.Length then
                        tweetString <- tweetString + hashtags.[randomHashtagId]
                    selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                    let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                    let json = Json.serialize data
                    destinationRef <! json
                    allTweetsSent <- allTweetsSent + 1
                    if allTweetsSent >= totalTweetsToBeSent then
                        ALL_COMPUTATIONS_DONE <- 1

        | "GetNumNodes" ->
            let totalNodes = message.Message |> int
            let dummyId = message.Author
            let mutable randomMessageId = random.Next(tweets.Length)
            let mutable randomHashtagId = random.Next(hashtags.Length*2)
            if randomMessageId < tweets.Length then
                let mutable tweetString = tweets.[randomMessageId]
                if randomHashtagId < hashtags.Length then
                    tweetString <- tweetString + hashtags.[randomHashtagId]
                let randomUserNameId = random.Next(totalNodes)
                if randomUserNameId < totalNodes then
                    let mutable randomUserName = sprintf "@User%i" randomUserNameId
                    tweetString <- tweetString + randomUserName
                selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                let json = Json.serialize data
                destinationRef <! json

        | "ReceiveTweet" ->
            let newTweet = message.Message
            Console.WriteLine("[{0}]==>TWEET: @{1} ==> {2}", selfStopwatch.Elapsed.TotalSeconds, message.Author, newTweet)
            receivedTweets <- Array.concat [| receivedTweets ; [|newTweet|] |]

        | "RetweetInit" ->
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "Retweet"}
            let json = Json.serialize data
            destinationRef <! json

        | "RetweetReceive" ->
            let newTweet = message.Message
            Console.WriteLine("[{0}]==>RETWEET: @{0} ==> {1}", selfStopwatch.Elapsed.TotalSeconds, message.Author, newTweet)
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = newTweet; Operation = "Tweet"}
            let json = Json.serialize data
            destinationRef <! json

        | "SubscribeInit" ->
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "Subscribe"}
            let json = Json.serialize data
            destinationRef <! json

        | "QueryInit" ->
            let mutable randomQueryId = random.Next(queries.Length)
            if randomQueryId < queries.Length then
                if queries.[randomQueryId] = "QuerySubscribedTweets" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QuerySubscribedTweets"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                elif queries.[randomQueryId] = "QueryHashtags" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QueryHashtags"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                elif queries.[randomQueryId] = "QueryMentions" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QueryMentions"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                

        | "QuerySubscribedTweets" ->
            let mutable randomSearchId = random.Next(search.Length)
            if randomSearchId < search.Length then
                let mutable randomsearchString = search.[randomSearchId]
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = randomsearchString; Operation = "QuerySubscribedTweets"}
                let json = Json.serialize data
                destinationRef <! json

        | "ReceiveQuerySubscribedTweets" ->
            let searchString = message.Author
            let searchTweetResults = message.Message
            Console.WriteLine("[{0}]==>QuerySubscribedTweets: Search:{1} ==> {2}", selfStopwatch.Elapsed.TotalSeconds, searchString, searchTweetResults)

        | "QueryHashtags" ->
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let mutable randomHashtagId = random.Next(hashtags.Length)
            if randomHashtagId < hashtags.Length then
                let data = {Author = selfId |> string; Message = hashtags.[randomHashtagId]; Operation = "QueryHashtags"}
                let json = Json.serialize data
                destinationRef <! json

        | "ReceiveQueryHashtags" ->
            let searchHashtag = message.Author
            let tweetsFound = message.Message
            Console.WriteLine("[{0}]==>QuerySubscribedTweets: Search:{1} ==> {2}", selfStopwatch.Elapsed.TotalSeconds, searchHashtag, tweetsFound)

        | "QueryMentions" ->
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "QueryMentions"}
            let json = Json.serialize data
            destinationRef <! json

        | "ReceiveQueryMentions" ->
            let tweetsFound = message.Message
            Console.WriteLine("[{0}]==>ReceiveQueryMentions: ==> {1}", selfStopwatch.Elapsed.TotalSeconds, tweetsFound)

        | _ -> 0|>ignore


        return! loop()
    }
    loop ()

let MybossActor (numNodesVal:int) (numTweetsVal:int) (operation:string) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let modeOfOperation = operation
    let selfStopwatchBoss = System.Diagnostics.Stopwatch()
    let mutable oldTimeBoss = 0.0
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with

        | StartBoss ->
            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                let mutable userActor = spawn system workerName (MyUserActor workerName i modeOfOperation)
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (i |> string)) system
                let data = {Author = ""; Message = workerName; Operation = "Register"}
                let json = Json.serialize data
                destinationRef <! json

            selfStopwatchBoss.Start()
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 60.0 do
                0|> ignore

            //Thread.Sleep(15000)
            
            // Send signal to all users to start their processes
            for i in 0..numNodes-1 do
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (i |> string)) system
                let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
                let json = Json.serialize data
                destinationRef <! json

            Console.WriteLine("Done with users")

            mailbox.Self <! SimulateBoss
            

        | SimulateBoss ->

            // choose random num/100 nodes and make them offline
            
            for i in 0..numNodes/100 do
                let mutable offlineNodeId = random.Next(numNodes)
                Console.WriteLine("[{0}]==> Setting {1} offline", selfStopwatchBoss.Elapsed.TotalSeconds, offlineNodeId)
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = offlineNodeId |> string; Message = ""; Operation = "GoOffline"}
                let json = Json.serialize data
                destinationRef <! json                

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 1.0 do
                0|> ignore
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)
            mailbox.Self <! SimulateBoss

        return! loop()
    }
    loop ()


let main argv =
    let numNodes = ((Array.get argv 1) |> int)
    let numTweets = ((Array.get argv 2) |> int)
    let modeOfOperation = ((Array.get argv 3) |> string)
    totalUsers <- numNodes
    totalTweetsToBeSent <- numTweets
    let bossActor = spawn system "bossActor" (MybossActor numNodes numTweets modeOfOperation)
    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/bossActor") system
    destinationRef <! StartBoss

    Console.WriteLine("Done with boss")

    while(ALL_COMPUTATIONS_DONE = 0) do
        0|>ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs
