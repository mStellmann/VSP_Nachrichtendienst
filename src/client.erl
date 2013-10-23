%% Copyright
-module(client).
-author("Matthias Stellmann and Grzegorz Markiewicz").

%% API
-export([startClients/0]).

%% Start all Clients
startClients() ->
% Read the cfg-File
  {ok, ConfigList} = file:consult("client.cfg"),
  {ok, NumberOfClients} = werkzeug:get_config_value(clients, ConfigList),
  {ok, LifetimeInSeconds} = werkzeug:get_config_value(lifetime, ConfigList),
  {ok, SendintervalInSeconds} = werkzeug:get_config_value(sendeintervall, ConfigList),
  {ok, {Servername, ServerNode}} = werkzeug:get_config_value(servername, ConfigList),

  ServerPID = {Servername, ServerNode},
  io:format("Serverinfo: Server at ~p with PID ~p~n", [ServerNode, ServerPID]),

  Sendinterval = timer:seconds(SendintervalInSeconds),
  Lifetime = timer:seconds(LifetimeInSeconds),
  createClients(lists:seq(1, NumberOfClients), ServerPID, Sendinterval, Lifetime).

%% This Function spawnes the clients
%% returns a List of the spawend PID for each Client
createClients(ClientIDList, ServerPID, Sendinterval, Lifetime) ->
  lists:map(fun(ClientID) ->
    ClientPID = spawn(fun() -> sendMessages(ClientID, ServerPID, Sendinterval, 0, []) end),
    io:format("INFO: Client with NR: ~p PID: ~p startet at ~p~n", [ClientID, ClientPID, werkzeug:timeMilliSecond()]),
    timer:send_after(Lifetime, ClientPID, endOfLifetime),
    ClientPID end,
%% 2. Parameter of the lists:map/2 Function
    ClientIDList)
.

%% editorMode - asking the Server for new MessageNumbers and sending Messages to the Server
sendMessages(ClientID, ServerPID, Sendinterval, NumberOfSendMsg, OwnMessageNumbers) ->
  ServerPID ! {getmsgid, self()},
  receive
    endOfLifetime -> exitClient(ClientID);

    {nid, Number} ->
      %% After sending 5 Messages, forget to send a Message and switch to Readermode
      case NumberOfSendMsg of
        5 ->
          logMessage(ClientID, lists:concat(["forgot to send MessageNr: ", Number, " at", werkzeug:timeMilliSecond(), " ~n"])),
          getMessages(ClientID, ServerPID, OwnMessageNumbers, Sendinterval);
        _ ->
          %% lists:concat([Number, ".Message: ", getHost()," ", ClientNr," G2/T13", " Send: ", werkzeug:timeMilliSecond()])
          Message = lists:concat([Number, ".-Message: C out - ", werkzeug:timeMilliSecond(), "(", getHostname(), ") ;"]),
          ServerPID ! {dropmessage, {Message, Number}},
          logMessage(ClientID, lists:concat(["send Message: ", Message, "~n"])),
          timer:sleep(Sendinterval),
          sendMessages(ClientID, ServerPID, Sendinterval, NumberOfSendMsg + 1, lists:append(OwnMessageNumbers, [Number]))
      end;

    Any ->
      logMessage(ClientID, lists:concat(["received anything not understandable: ", Any, "~n"])),
      sendMessages(ClientID, ServerPID, Sendinterval, NumberOfSendMsg, OwnMessageNumbers)
  end
.

%% readerMode - receiving Messages from Server and saving them into the .log-File
getMessages(ClientID, ServerPID, OwnMessageNumbers, SendInterval) ->
  ServerPID ! {getmessages, self()},
  receive
    endOfLifetime -> exitClient(ClientID);

    {reply, NNr, Message, Terminated} ->
%% checking if received Message is from our own editor
      case lists:any(fun(Elem) -> Elem == NNr end, OwnMessageNumbers) of
        true -> logMessage(ClientID, lists:concat(["Received a Message of my own: ", Message, "at ", werkzeug:timeMilliSecond(), "~n"]));
        false -> logMessage(ClientID, lists:concat(["Received a Message: ", Message, "at ", werkzeug:timeMilliSecond(), "~n"]))
      end,
      case Terminated of
%% switching back to editormode if Terminated is true with a new calculated SendInterval
        true ->
          NewWaitingtime = calculateNewWaitingtime(SendInterval),
          sendMessages(ClientID, ServerPID, NewWaitingtime, 0, OwnMessageNumbers);
%% more messages to receive
        false -> getMessages(ClientID, ServerPID, OwnMessageNumbers, SendInterval)
      end;

    Any ->
      logMessage(ClientID, lists:concat(["received anything not understandable: ", Any, "~n"])),
      getMessages(ClientID, ServerPID, OwnMessageNumbers, SendInterval)
  end
.

%% Exits the given Client and logs it
exitClient(ClientID) ->
  io:format("INFO: Client with NR: ~p PID: ~p ended at ~p~n", [ClientID, self(), werkzeug:timeMilliSecond()]),
  erlang:exit("EndOfLife")
.

%% Generates a new Timeinterval
%% generated interval is always 50 percent '<' or '>' as the 'CurrentInterval' with a minimum of +-1
%% min: 2
%% returns the waiting time to send a message
calculateNewWaitingtime(CurrentInterval) ->
  case CurrentInterval =< 3 of
    true ->
      NewInterval = CurrentInterval * 1.5,
      erlang:round(NewInterval);
    false ->
      UpOrDown = random:uniform(2),
      case UpOrDown == 1 of
      %% 1 == Down
        true ->
          NewInterval = CurrentInterval * 0.5,
          erlang:round(NewInterval);
      %% 2 == UP
        false ->
          NewInterval = CurrentInterval * 1.5,
          erlang:round(NewInterval)
      end
  end
.

getHostname() ->
  {ok, Hostname} = inet:gethostname(),
  Hostname
.

%% simplifies the Logging
logMessage(ClientNr, Message) ->
  Filename = lists:concat(["client_", ClientNr, "@", getHostname(), ".log"]),
  werkzeug:logging(Filename, lists:concat(["[", werkzeug:timeMilliSecond(), "] ", ClientNr, ": ", Message, "\n"]))
.