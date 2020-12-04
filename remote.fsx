#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
#r "nuget: FSharp.Json"
#load @"./Constants.fsx"
#load @"./MessageTypes.fsx"

open Constants.Constants
open System.Text.RegularExpressions
open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration
open MessageTypes
open FSharp.Json

let config =
    Configuration.parse
        @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.helios.tcp {
                hostname = ""127.0.0.1""
                port = 9002
            log-dead-letters-during-shutdown = off
            log-dead-letters = off
            }
        }"
let system = System.create "Twitter" config

let MyengineActor (numNodesVal:int) (numTweetsVal:int) (operation:string) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let modeOfOperation = operation
    let mutable subscribers = Map.empty
    let mutable tweetsToBeSent = Map.empty
    let mutable allTweets = Map.empty
    let mutable userSubscribedTweets = Map.empty
    let mutable myMentions = Map.empty
    let mutable offlineUsers = Array.empty
    let mutable hashtagTweets = Map.empty
    let mutable tweetsReceived = 0
    let selfStopwatchEngine = System.Diagnostics.Stopwatch()

    let searchMentions newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.create 0 ""
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '@' then
                userFound <- 1
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                newUser <- newUser.Trim()
                if newUser <> "" then
                    userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '@' then
                    userFound <- 1
            elif userFound = 1 && c <> '@' then
                newUser <- newUser + string(c)
        newUser <- newUser.Trim()
        if newUser <> "" then
            userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
        userTweetNumber

    let searchHashtags newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.create 0 ""
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '#' then
                userFound <- 1
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                newUser <- newUser.Trim()
                if newUser <> "" then
                    userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '#' then
                    userFound <- 1
            elif userFound = 1 && c <> '#' then
                newUser <- newUser + string(c)
        newUser <- newUser.Trim()
        if newUser <> "" then
            userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
        userTweetNumber

    let searchTweets (receivedTweets:string[]) (searchString:string)=
        let mutable searchedTweets = Array.create 0 ""
        for newTweets in receivedTweets do
            let mutable wordIndex = newTweets.IndexOf(searchString)
            if wordIndex <> -1 then
                searchedTweets <- Array.concat [| searchedTweets ; [|newTweets|] |]
        searchedTweets
                
    let matchSample r m =
        let r = Regex(r)
        let m1 = r.Match m
        let idFound = m1.Groups.[1] |> string |> int
        idFound

    let stripchars chars str =
        Seq.fold
            (fun (str: string) chr ->
            str.Replace(chr |> Char.ToUpper |> string, "").Replace(chr |> Char.ToLower |> string, ""))
            str chars

    
    let rec loop() = actor {
        let! json = mailbox.Receive()
        let message = Json.deserialize<TweetApiMessage> json
        let sender = mailbox.Sender()
        let operation = message.Operation
        
        match operation with
        | "StartEngine" ->
            Console.WriteLine("Initializing all users...")
            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                subscribers <- subscribers.Add(workerName, [|-1|])
                tweetsToBeSent <- tweetsToBeSent.Add(workerName, [|{Author = ""; Message = ""}|])
                allTweets <- allTweets.Add(workerName, [|""|])
                myMentions <- myMentions.Add(workerName, [|""|])
                userSubscribedTweets <- userSubscribedTweets.Add(workerName, [|""|])
                // add subscribers
                let mutable userSubscribers = [|0|]
                if modeOfOperation = "zipf" then
                    for j in 1..(numNodes/(i+2)) do
                        userSubscribers <- Array.concat [| userSubscribers ; [|j|] |]
                    printfn "%d: %A" i userSubscribers
                subscribers <- subscribers.Add(workerName, userSubscribers)

            hashtagTweets <- hashtagTweets.Add("", [|""|])
            Console.WriteLine("Initialized all users!")
            selfStopwatchEngine.Start()

        | "GetNumNodes" ->
            let userId = message.Author |> int
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
            let data = {Author = userId |> string; Message = numNodes |> string; Operation = "GetNumNodes"}
            let json = Json.serialize data
            destinationRef <! json

        | "Tweet" ->
            let userId = message.Author |> int
            let tweetString = message.Message
            tweetsReceived <- tweetsReceived + 1
            Console.WriteLine("[{0}]==>TWEET: User{1} @ {2}", string(selfStopwatchEngine.Elapsed.TotalSeconds), userId, tweetString)
            if tweetsReceived = numTweets then
                for userId in 0..numNodes-1 do
                    let mutable userName = sprintf "User%i" userId
                    if userSubscribedTweets.ContainsKey(userName) then
                        let mutable totalTweetsReceived = userSubscribedTweets.[userName]
                        let mutable totalTweetsReceivedLength = totalTweetsReceived.Length
                        Console.WriteLine("({0},{1})", userId, totalTweetsReceivedLength)
                ALL_COMPUTATIONS_DONE <- 1
            if userId < numNodes then
                let mutable userName = sprintf "User%i" userId

                // store tweet for user
                let mutable allTweetsByUser = allTweets.[userName]
                allTweetsByUser <- Array.concat [| allTweetsByUser ; [|tweetString|] |]
                allTweets <- allTweets.Add(userName, allTweetsByUser)

                // store hashtags for tweet
                let userHashtags = searchHashtags tweetString
                for hashtags in userHashtags do
                    if hashtagTweets.ContainsKey(hashtags) then
                        let mutable thisHashtagTweets = hashtagTweets.[hashtags]
                        thisHashtagTweets <- Array.concat [| thisHashtagTweets ; [|tweetString|] |]
                        hashtagTweets <- hashtagTweets.Add(hashtags, thisHashtagTweets)
                    else
                        hashtagTweets <- hashtagTweets.Add(hashtags, [|tweetString|]) 

                // store user mentions for tweet
                let userMentions = searchMentions tweetString
                for mentioned in userMentions do
                    let mutable myMentionedTweets = myMentions.[mentioned]
                    myMentionedTweets <- Array.concat [| myMentionedTweets ; [|tweetString|] |]
                    myMentions <- myMentions.Add(mentioned, myMentionedTweets)

                // who should this tweet be sent out to?
                if subscribers.ContainsKey(userName) then
                    let mutable allSubscribers = subscribers.[userName]
                    allSubscribers <- allSubscribers |> Array.filter ((<>) -1 )
                    
                    for mentioned in userMentions do
                        let mentionedId = matchSample userRegexMatch mentioned
                        if mentionedId < numNodes then
                            allSubscribers <- allSubscribers |> Array.filter ((<>) mentionedId )
                            allSubscribers <- Array.concat [| allSubscribers ; [|mentionedId|] |]

                    for subs in allSubscribers do
                        let mutable destination = sprintf "User%i" subs
                        // store in user subscribed tweets
                        let mutable newUserSubTweets = userSubscribedTweets.[destination]
                        newUserSubTweets <- Array.concat [| newUserSubTweets ; [|tweetString|] |]
                        userSubscribedTweets <- userSubscribedTweets.Add(destination, newUserSubTweets)

                        // should we send it or not?
                        let mutable userFoundOffline = false
                        let tweet = {Author = userName; Message = tweetString}
                        for offlineUsersCurrent in offlineUsers do
                            if not userFoundOffline then
                                if offlineUsersCurrent = subs then
                                    userFoundOffline <- true
                        if userFoundOffline then
                            let mutable usertweetsToBeSent = tweetsToBeSent.[destination]
                            usertweetsToBeSent <- Array.concat [| usertweetsToBeSent ; [|tweet|] |]
                            tweetsToBeSent <- tweetsToBeSent.Add(destination, usertweetsToBeSent)
                        else
                            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (subs |> string)) system
                            let data = {Author = userName |> string; Message = tweetString; Operation = "ReceiveTweet"}
                            let json = Json.serialize data
                            destinationRef <! json
                    
        | "Retweet" ->
            // choose a random user and ask them for a random tweet
            let userId = message.Author |> int
            let mutable randomUserId = random.Next(numNodes)
            let mutable randomUserName = sprintf "User%i" randomUserId
            let allRandomUserTweets = allTweets.[randomUserName]
            let randomTweetNumber = random.Next(allRandomUserTweets.Length)
            if randomTweetNumber < allRandomUserTweets.Length then
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
                let data = {Author = "" |> string; Message = allRandomUserTweets.[randomTweetNumber]; Operation = "ReceiveTweet"}
                let json = Json.serialize data
                destinationRef <! json

        | "Subscribe" ->
            let userId = message.Author |> int
            let mutable allUsers = [|0..numNodes-1|]
            let mutable userName = sprintf "User%i" userId
            if userId < numNodes then
                if subscribers.ContainsKey(userName) then
                    let mutable userSubscribers = subscribers.[userName]
                    userSubscribers <- userSubscribers |> Array.filter ((<>) -1 )
                    if userSubscribers.Length < numNodes - 2 then
                        // remove already subscribed indexes and choose from among the remaining ones
                        for i in userSubscribers do
                            allUsers <- allUsers |> Array.filter ((<>) i )
                        allUsers <- allUsers |> Array.filter ((<>) userId )
                        let mutable randomNewSub = random.Next(allUsers.Length)
                        userSubscribers <- Array.concat [| userSubscribers ; [|allUsers.[randomNewSub]|] |] 
                        subscribers <- subscribers.Add(userName, userSubscribers)
                        Console.WriteLine("[{0}]==>SUBSCRIBE: User{1} to User{2}", string(selfStopwatchEngine.Elapsed.TotalSeconds), userId, randomNewSub)

        | "GoOffline" ->
            Console.WriteLine("GoOffline")
            let userId = message.Author |> int
            offlineUsers <- offlineUsers |> Array.filter ((<>) userId )
            offlineUsers <- Array.concat [| offlineUsers ; [|userId|] |] 
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
            let data = {Author = userId |> string; Message = ""; Operation = "GoOffline"}
            let json = Json.serialize data
            destinationRef <! json

        | "GoOnline" ->
            Console.WriteLine("GoOnline")
            let userId = message.Author |> int
            let mutable userName = sprintf "User%i" userId
            if userId < numNodes then
                let mutable usertweetsToBeSent = tweetsToBeSent.[userName]
                offlineUsers <- offlineUsers |> Array.filter ((<>) userId )
                for tweet in usertweetsToBeSent do
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
                    let data = {Author = tweet.Author |> string; Message = tweet.Message; Operation = "ReceiveTweet"}
                    let json = Json.serialize data
                    destinationRef <! json
                tweetsToBeSent <- tweetsToBeSent.Add(userName, [|{Author = ""; Message = ""}|])

        | "QuerySubscribedTweets" ->
            let userId = message.Author |> int
            let searchString = message.Message
            let mutable userName = sprintf "User%i" userId
            let mutable newUserSubTweets = userSubscribedTweets.[userName]
            newUserSubTweets <- newUserSubTweets |> Array.filter ((<>) "" )
            let mutable tweetsFound = searchTweets newUserSubTweets searchString
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system

            if tweetsFound.Length <> 0 then
                let arr = Json.serialize tweetsFound    
                let data = {Author = searchString |> string; Message = arr; Operation = "ReceiveQuerySubscribedTweets"}
                let json = Json.serialize data
                destinationRef <! json
            else 
                let arr = Json.serialize [| "No tweets found" |]    
                let data = {Author = searchString |> string; Message = arr; Operation = "ReceiveQuerySubscribedTweets"}
                let json = Json.serialize data
                destinationRef <! json

        | "QueryHashtags" ->
            let userId = message.Author |> int  
            let hashtagQuery = message.Message
            if userId < numNodes then
                let hashtagString = stripchars "#" hashtagQuery
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
                if hashtagTweets.ContainsKey(hashtagString) then
                    let mutable tweetsFound = hashtagTweets.[hashtagString]
                    let arr = Json.serialize tweetsFound
                    let data = {Author = hashtagQuery |> string; Message = arr; Operation = "ReceiveQueryHashtags"}
                    let json = Json.serialize data
                    destinationRef <! json
                else
                    let arr = Json.serialize [| "No tweets found" |]    
                    let data = {Author = hashtagQuery |> string; Message = arr; Operation = "ReceiveQueryHashtags"}
                    let json = Json.serialize data
                    destinationRef <! json

        | "QueryMentions" ->
            let userId = message.Author |> int  
            let mutable userName = sprintf "User%i" userId
            let mutable userMentions = myMentions.[userName]
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (userId |> string)) system
            
            if userMentions.Length <> 0 then
                let arr = Json.serialize userMentions    
                let data = {Author = "" |> string; Message = arr; Operation = "ReceiveQueryMentions"}
                let json = Json.serialize data
                destinationRef <! json
            else
                let arr = Json.serialize [| "No tweets found" |]    
                let data = {Author = "" |> string; Message = arr; Operation = "ReceiveQueryMentions"}
                let json = Json.serialize data
                destinationRef <! json
            
        | _ -> 
            printfn "Invalid Request"
            sender <! "Invalid Request"

        return! loop()
    }
    loop ()


let main argv =
    let numNodes = ((Array.get argv 1) |> int)
    let numTweets = ((Array.get argv 2) |> int)
    let modeOfOperation = ((Array.get argv 3) |> string)

    let engineActor = spawn system "engineActor" (MyengineActor numNodes numTweets modeOfOperation)
    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
    let data = {Author = ""; Message = ""; Operation = "StartEngine"}
    let json = Json.serialize data
    destinationRef <! json

    Console.WriteLine("Done with engineActor")

    while(ALL_COMPUTATIONS_DONE = 0) do
        0|>ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs