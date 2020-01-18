defmodule Brex.Channel do

  def make() do
    tag = make_ref()
    pid = spawn(fn ->
      loop_chan(tag, :queue.new(), :queue.new(), nil)
    end)

    {:ok, {pid, tag}}
  end

  def post({pid, tag}, value) do
    send(pid, {:post, {tag, value}})
    :ok
  end

  def close({pid, tag}) do
    reply_tag = make_ref()
    send(pid, {:close, {tag, self(), reply_tag}})
    receive do
      {:closed, ^reply_tag} -> :ok
    end
  end

  def pop({pid, tag}) do
    reply_tag = make_ref()
    send(pid, {:sub, {tag, self(), reply_tag}})

    receive do
      {:reply, ^reply_tag, result} -> {:ok, result}
      {:error, {^reply_tag, :channel_closed}} -> {:error, :channel_closed}
    end
  end

  defp loop_chan(tag, subs, messages, stash) do
    r = case {:queue.out(subs), :queue.out(messages), stash} do
      {{{:value, {pid, reply_tag, :reply}}, sub_rest}, {{:value, m}, msg_rest}, nil} ->
        send(pid, {:reply, reply_tag, m})
        loop_chan(tag, sub_rest, msg_rest, nil)
        :halt
      {{{:value, {pid, reply_tag, :ask}}, sub_rest}, {{:value, m}, msg_rest}, nil} ->
        send(pid, {:unack, {reply_tag, self(), tag, m}})
        loop_chan(tag, sub_rest, msg_rest, m)
        :halt
      _ -> :cont
    end

    if r == :cont do
      receive do
        {:close, {^tag, caller, reply_tag}} ->
          subs
          |> :queue.to_list # fml
          |> Enum.map(fn {sub_pid, sub_reply_tag, _}->
            # send errors to subscribers.
            send(sub_pid, {:error, {sub_reply_tag, :channel_closed}})
          end)
          send(caller, {:closed, reply_tag})
          # do not loop
        {:sub, {^tag, caller, reply_tag}} ->
          loop_chan(tag, :queue.in({caller, reply_tag, :reply}, subs), messages, stash)
        {:ask, {^tag, caller, reply_tag}} ->
          loop_chan(tag, :queue.in({caller, reply_tag, :ask}, subs), messages, stash)
        {:ack, ^tag} ->
          loop_chan(tag, subs, messages, nil)
        {:nack, ^tag} ->
          loop_chan(tag, subs, :queue.in_r(stash, messages), nil)
        {:post, {^tag, value}} ->
          loop_chan(tag, subs, :queue.in(value, messages), stash)
      end
    end
  end
end
