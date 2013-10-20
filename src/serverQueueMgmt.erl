%% Copyright
-module(serverQueueMgmt).
-author("Matthias Stellmann and Grzegorz Markiewicz").

%% API
-export([start/2]).

%% starting the QueueMgmt and initializing the queues
start(DLQLimit, ClientLifetime) ->
  erlang:spawn(fun() -> queueMgmt(DLQLimit, [], [], [], ClientLifetime) end)
.


%% TODO -> Comments, Comments, Comments, ..... , Comments
%% TODO -> Fehlernachricht - DummyMessage wird an die Clients versendet..
%% TODO -> Wahrscheinlich wird die LMNS nicht richtig ausgelesen bzw. berechnet.. <- sehr wahrscheinlich falsche berechnung der LMNS

%% receiving loop for the QueueMgmt
queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime) ->
  receive
    endOfLife -> exitQueueMgmt();

    {endOfClientLifeTime, ClientPID} -> clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    {dropmessage, {Message, Number}} -> dropMessageInHBQ({Number, Message}, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

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
  NewHBQ = werkzeug:pushSL(HBQ, {MsgNr, lists:concat([Message, " S in HBQ at ", werkzeug:timeMilliSecond()])}),
  case werkzeug:lengthSL(NewHBQ) > DLQLimit / 2 of
    true ->
      logToFile(lists:concat([werkzeug:timeMilliSecond(), ":  ", " pushed in DLQ"])),
      pushMessagesInDLQ(NewHBQ, DLQ, ClientList, ClientLifetime, DLQLimit, true);
    _ -> queueMgmt(DLQLimit, NewHBQ, DLQ, ClientList, ClientLifetime)
  end
.

pushMessagesInDLQ(HBQ, DLQ, ClientList, ClientLifetime, DLQLimit, NewPushingFlac) ->
  case werkzeug:notemptySL(HBQ) of
    false ->
      logToFile(lists:concat([werkzeug:timeMilliSecond(), ":  ", " HBQ is  empty"])),
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
    _ -> doNothing
  end,
%% missing messages -> failuremessage will be created and pushed in DLQ
  DLQisNotEmptyFlac = werkzeug:notemptySL(DLQ),
  MaxNrDLQ = werkzeug:maxNrSL(DLQ),
  MinNrHBQ = werkzeug:minNrSL(HBQ),
  MaxNrDLQplusTwo = MaxNrDLQ + 2,
  MaxNrDLQplusOne = MaxNrDLQ + 1,
  MinNrHBQminusOne = MinNrHBQ - 1,

  io:format(" Flac: ~p MaxNrDLQ: ~p MinNrHBQ: ~p~n", [DLQisNotEmptyFlac, MaxNrDLQ, MinNrHBQ]),
  case MaxNrDLQplusTwo =< MinNrHBQ of
    true when NewPushingFlac andalso DLQisNotEmptyFlac ->
      logToFile(lists:concat([werkzeug:timeMilliSecond(), ":  ", " FailMessage created"])),
      failMessage = lists:concat(["*** missing messages from ", MaxNrDLQplusOne, " to ", MinNrHBQminusOne, " at ", werkzeug:timeMilliSecond(), " ***"]),
      case isDLQFull(DLQ, DLQLimit) of
        true ->
          case isDLQPushPossible(DLQ, ClientList) of
            false -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
            _ ->
              NewDLQ = werkzeug:popSL(DLQ),
              NewPushedDLQ = werkzeug:pushSL(NewDLQ, {MinNrHBQminusOne, failMessage}),
              pushMessagesInDLQ(HBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false)
          end;
        _ ->
          NewDLQ = werkzeug:pushSL(DLQ, {MinNrHBQminusOne, failMessage}),
          pushMessagesInDLQ(HBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end;

%% next missing messages in HBQ, stop pushing
    true when not NewPushingFlac andalso DLQisNotEmptyFlac ->
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);

%% no missing messages
    _ ->
      case isDLQFull(DLQ, DLQLimit) of
        true ->
          case isDLQPushPossible(DLQ, ClientList) of
            false -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
            _ ->
              NewDLQ = werkzeug:popSL(DLQ),
              {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
              NewHBQ = werkzeug:popSL(HBQ),
              NewPushedDLQ = werkzeug:pushSL(NewDLQ, {MsgNumber, lists:concat([Message, " S in DLQ at ", werkzeug:timeMilliSecond()])}),
              pushMessagesInDLQ(NewHBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false)
          end;
        _ ->
          {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
          NewHBQ = werkzeug:popSL(HBQ),
          NewDLQ = werkzeug:pushSL(DLQ, {MsgNumber, lists:concat([Message, " S in DLQ at ", werkzeug:timeMilliSecond()])}),
          pushMessagesInDLQ(NewHBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end
  end
.

isDLQPushPossible(DLQ, ClientList) ->
  List = [MessageNumber || {_, MessageNumber, _} <- ClientList],
  case List == [] of
    true -> true;
    _ -> ListMinCL = lists:min(List),
      ListMinCLplusOne = ListMinCL + 1,
      MinDLQ = werkzeug:minNrSL(DLQ),
      case ListMinCLplusOne == MinDLQ of
        true -> false;
        _ -> true
      end
  end
.

isDLQFull(DLQ, DLQLimit) ->
  case werkzeug:lengthSL(DLQ) == DLQLimit of
    true -> true;
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
      case SNr == werkzeug:maxNrSL(DLQ) of
        true ->
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
  ClientPIDTuple = [{Elem, LastMessageNumberSend} || {Elem, LastMessageNumberSend, _} <- ClientList, Elem == ClientPID],
  case ClientPIDTuple == [] of
    true -> 0;
    _ ->
      [{_, MessageNumber}] = ClientPIDTuple,
      MessageNumber
  end
.

%% adding a new messagenumber and starting a new timer for the client
clientMgmt_newMessageNumberOfClient(ClientPID, LastMessagenumberSend, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  ClientElem = [{Elem, LMNS, Timer} || {Elem, LMNS, Timer} <- ClientList, Elem == ClientPID],
  case ClientElem == [] of
    false ->
      [{_, _, Timer}] = ClientElem,
      [ClientVar] = ClientElem,
      NewTimer = werkzeug:reset_timer(Timer, ClientLifetime, {endOfClientLifeTime, ClientPID}),
      DelClientList = lists:delete(ClientVar, ClientList),
      NewClientList = DelClientList ++ [{ClientPID, LastMessagenumberSend, NewTimer}],
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime);

    true ->
      {ok, Timer} = timer:send_after(ClientLifetime, {endOfClientLifeTime, ClientPID}),
      NewClientList = ClientList ++ [{ClientPID, LastMessagenumberSend, Timer}],
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
  end
.

%% deleting the client out of the ClientList if the client timeouts
clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  ClientElem = [{Elem, LMNS, Timer} || {Elem, LMNS, Timer} <- ClientList, Elem == ClientPID],
  case ClientElem == [] of
    true -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
    _ ->
      [ClientVar] = ClientElem,
      NewClientList = lists:delete(ClientVar, ClientList),
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
  end

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