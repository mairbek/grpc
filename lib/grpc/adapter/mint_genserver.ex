defmodule GRPC.Adapter.Mint.ConnectionProcess do

  use GenServer

  require Logger

  defstruct [:conn, requests: %{}]

  def start_link({scheme, host, port, options}) do
    GenServer.start_link(__MODULE__, {scheme, host, port, options})
  end

  def request(pid, method, path, headers, body) do
    GenServer.call(pid, {:request, method, path, headers, body})
  end

  def await() do
  end


  ## Callbacks

  @impl true
  def init({scheme, host, port, options}) do
    case Mint.HTTP2.connect(scheme, host, port, options) do
      {:ok, conn} ->
        state = %__MODULE__{conn: conn}
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:request, method, path, headers, body}, from, state) do
    # In both the successful case and the error case, we make sure to update the connection
    # struct in the state since the connection is an immutable data structure.
    case Mint.HTTP2.request(state.conn, method, path, headers, body) do
      {:ok, conn, request_ref} ->
        state = put_in(state.conn, conn)
        # We store the caller this request belongs to and an empty map as the response.
        # The map will be filled with status code, headers, and so on.
        {:ok, chan} = Brex.Channel.make()
        state = put_in(state.requests[request_ref], %{from: from, chan: chan})
        {:reply, {:ok, chan}, state}

      {:error, conn, reason} ->
        state = put_in(state.conn, conn)
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(message, state) do
    # We should handle the error case here as well, but we're omitting it for brevity.
    case Mint.HTTP2.stream(state.conn, message) do
      :unknown ->
        _ = Logger.error(fn -> "Received unknown message: " <> inspect(message) end)
        {:noreply, state}

      {:ok, conn, responses} ->
        IO.puts("RECEIVED #{inspect(responses)}")
        state = put_in(state.conn, conn)
        state = Enum.reduce(responses, state, &process_response/2)
        {:noreply, state}
    end
  end

  defp process_response({:status, request_ref, status}, state) do
    chan = state.requests[request_ref][:chan]
    Brex.Channel.post(chan, {:status, status})
    state
  end

  defp process_response({:headers, request_ref, headers}, state) do
    chan = state.requests[request_ref][:chan]
    Brex.Channel.post(chan, {:headers, headers})
    state
  end

  defp process_response({:data, request_ref, new_data}, state) do
    chan = state.requests[request_ref][:chan]
    Brex.Channel.post(chan, {:data, new_data})
    state
  end

  defp process_response({:done, request_ref}, state) do
    chan = state.requests[request_ref][:chan]
    Brex.Channel.post(chan, {:done, []})
    state
  end

  # A request can also error, but we're not handling the erroneous responses for
  # brevity.

end
