pub fn print_json_lines<T, I>(items: I) -> anyhow::Result<()>
where
    I: Iterator,
    T: From<I::Item> + serde::Serialize,
{
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    for item in items {
        serde_json::to_writer(&mut stdout, &T::from(item))?;
        println!();
    }
    Ok(())
}

pub fn print_json(item: &impl serde::Serialize) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), item)?;
    println!();
    Ok(())
}
