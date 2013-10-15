%% Copyright
-module(serverQueueMgmt).
-author("Matthias Stellmann and Grzegorz Markiewicz").

%% API
-export([start/2]).

%% starting the QueueMgmt and initializing the queues
%% returns the pid of this process
start(DLQLimit, ClientLifetime) ->
  erlang:spawn(fun() -> queueMgmt(DLQLimit, [], [], [], ClientLifetime) end)
.

%% receiving loop for the QueueMgmt
queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime) ->
  receive
    endOfLife -> exitQueueMgmt();

    {endOfClientLifeTime, ClientPID} -> clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    {dropmessages, {Message, Number}} -> dropMessageInHBQ({Message, Number}, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    {getmessages, ClientPID} ->
      LastSendMessagenumber = clientMgmt_lastMessageNumberSend(ClientPID, ClientList),
      NewSendMessagenumber = sendMessageToClient(ClientPID, LastSendMessagenumber, DLQ),
      clientMgmt_newMessageNumberOfClient(ClientPID, NewSendMessagenumber, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    Any ->
      logToFile(lists:concat(["received anything not understandable: ", Any, "~n"])),
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime)
  end
.

%% starting the master algo
dropMessageInHBQ(MessageTuple, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  {MsgNr, Message} = MessageTuple,
  NewHBQ = werkzeug:pushSL(HBQ, {MsgNr, lists:concat([Message, " S in HBQ at " + werkzeug:timeMilliSecond()])}),
  if
    werkzeug:lengthSL(NewHBQ) > DLQLimit / 2 -> pushMessagesInDLQ(NewHBQ, DLQ, ClientList, ClientLifetime, DLQLimit, true);
    _ -> queueMgmt(DLQLimit, NewHBQ, DLQ, ClientList, ClientLifetime)
  end
.

pushMessagesInDLQ(HBQ, DLQ, ClientList, ClientLifetime, DLQLimit, NewPushingFlac) ->
  if
    werkzeug:notemptySL(HBQ) == false -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime)
  end,

  if
  %% missing messages -> failuremessage will be created and pushed in DLQ
    werkzeug:maxNrSL(DLQ) + 2 =< werkzeug:minNrSL(HBQ) and NewPushingFlac == true ->
      failMessage = lists:concat(["*** missing messages from " + werkzeug:maxNrSL(DLQ) + 1, " to ", werkzeug:minNrSL(HBQ) - 1, " at ", werkzeug:timeMilliSecond(), " ***"]),
      if
        isDLQFull(DLQ, DLQLimit) == true ->
          if
            isDLQPushPossible(DLQ, ClientList) == false -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
            _ ->
              NewDLQ = werkzeug:popSL(DLQ),
              NewPushedDLQ = werkzeug:pushSL(NewDLQ, {werkzeug:minNrSL(HBQ) - 1, failMessage}),
              pushMessagesInDLQ(HBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false)
          end;
        _ ->
          NewDLQ = werkzeug:pushSL(DLQ, {werkzeug:minNrSL(HBQ) - 1, failMessage}),
          pushMessagesInDLQ(HBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end;

    %% next missing messages in HBQ, stop pushing
    werkzeug:maxNrSL(DLQ) + 2 =< werkzeug:minNrSL(HBQ) and NewPushingFlac == false ->
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);

    %% no missing messages
    _ ->
      if
        isDLQFull(DLQ, DLQLimit) == true ->
          if
            isDLQPushPossible(DLQ, ClientList) == false -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
            _ ->
              NewDLQ = werkzeug:popSL(DLQ),
              {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
              NewHBQ = werkzeug:popSL(HBQ),
              NewPushedDLQ = werkzeug:pushSL(NewDLQ, {MsgNumber, lists:concat([Message, " S in DLQ at " + werkzeug:timeMilliSecond()])}),
              pushMessagesInDLQ(NewHBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false)
          end;
        _ ->
          {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
          NewHBQ = werkzeug:popSL(HBQ),
          NewDLQ = werkzeug:pushSL(DLQ, {MsgNumber, lists:concat([Message, " S in DLQ at " + werkzeug:timeMilliSecond()])}),
          pushMessagesInDLQ(HBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end
  end
.

isDLQPushPossible(DLQ, ClientList) ->
  MinClientMsgNumber = lists:min([MessageNumber || {_, MessageNumber, _} <- ClientList]),
  if MinClientMsgNumber + 1 == werkzeug:minNrSL(DLQ) -> false;
    _ -> true
  end
.

isDLQFull(DLQ, DLQLimit) ->
  if
    werkzeug:lengthSL(DLQ) == DLQLimit -> true;
    _ -> false
  end
.
%% --------- ending the algo ----------

%% sending the next message to the client
sendMessageToClient(ClientPID, LastSendMessagenumber, DLQ) ->
  case werkzeug:findneSL(DLQ, LastSendMessagenumber) of
  %% no (new) message in DLQ to deliver to the client
    {-1, nok} ->
      ClientPID ! {reply, LastSendMessagenumber, lists:concat(["dummy message - no new messages in DLQ until yet (", werkzeug:timeMilliSecond(), ")"]), true},
      LastSendMessagenumber;

  %% next message found, sending to client
    {SNr, Elem} ->
      if
        SNr == werkzeug:maxNrSL(DLQ) ->
          ClientPID ! {reply, SNr, lists:concat([Elem, " S Out: ", werkzeug:timeMilliSecond()]), true},
          SNr;
        _ ->
          ClientPID ! {reply, SNr, lists:concat([Elem, " S Out: ", werkzeug:timeMilliSecond()]), false},
          SNr
      end
  end
.

%% getting the last messagenumber which was send to the client
clientMgmt_lastMessageNumberSend(ClientPID, ClientList) ->
  case lists:any(fun(Elem) -> Elem == {ClientPID, _, _} end, ClientList) of
    true ->
      {_, LastSendMessage, _} = Elem,
      LastSendMessage;
    false -> 0
  end
.

%% adding a new messagenumber and starting a new timer for the client
clientMgmt_newMessageNumberOfClient(ClientPID, lastMessagenumberSend, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  case lists:any(fun(Elem) -> Elem == {ClientPID, _, _} end, ClientList) of
    true ->
      {_, _, Timer} = Elem,
      NewTimer = werkzeug:reset_timer(Timer, ClientLifetime, {endOfClientLifeTime, ClientPID}),
      NewClientList = lists:delete({ClientPID, _, _}, ClientList) ++ {ClientPID, lastMessagenumberSend, NewTimer},
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime);

    false ->
      {ok, Timer} = timer:send_after(ClientLifetime, {endOfClientLifeTime, ClientPID}),
      NewClientList = ClientList ++ {ClientPID, lastMessagenumberSend, Timer},
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
  end
.

%% deleting the client out of the ClientList if the client timeouts
clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  NewClientList = lists:delete({ClientPID, _, _}, ClientList),
  queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
.


exitQueueMgmt() ->
  io:format("INFO: Server-QueueMgmt with PID: ~p ended at ~p~n", [self(), werkzeug:timeMilliSecond()]),
  erlang:exit("EndOfLife")
.


%% -- helping classes --
getHostname() ->
  {ok, Hostname} = inet:gethostname(),
  Hostname
.

logToFile(Message) ->
  Filename = lists:concat(["ServerQueueMgmt: ", "@", getHostname(), ".log"]),
  werkzeug:logging(Filename, lists:concat(["[", werkzeug:timeMilliSecond(), "] ", ": ", Message]))
.