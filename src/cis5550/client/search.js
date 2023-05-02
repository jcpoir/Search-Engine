async function search(event, page = 1) {
  event.preventDefault();

  // Clear any existing search results from the page
  const searchResults = document.getElementById("search-results");
  while (searchResults.firstChild) {
    searchResults.removeChild(searchResults.firstChild);
  }

  // Get the user's search query from the form input field
  const searchValue = document.getElementById("query").value;
  console.log(`Search query: ${searchValue}`);

  try {
    const itemsPerPage = 10;
    const startIndex = (page - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;

    // TODO: replace with correct url
    // // Replace the URL with your Java server's URL
    // const serverUrl = 'https://your-java-server-url.com';

    // // Use Axios to make a GET request to your Java server's API endpoint
    // const response = await axios.get(`${serverUrl}/api/search`, {
    //   params: {
    //     q: searchValue,
    //   },
    // });

    // const data = response.data;

    let data;
    if (searchValue == "2") {
      data = dummyData2;
    } else if (searchValue == "3") {
      data = dummyData3;
    } else if (searchValue == "long") {
      data = dummyDataLong;
    }
      else {
      data = dummyData1;
    }

    // console.log('results', data, startIndex, endIndex)
    const results = data.slice(startIndex, endIndex);
    const totalPages = Math.ceil(data.length / itemsPerPage);

    // Create a new unordered list to hold the search results
    const resultList = document.createElement("ul");

    // Loop through each search result and create a new list item for it
    results.forEach((result) => {
      console.log('result',result)
      // Create a new list item element
      const listItem = document.createElement("li");
      listItem.classList.add("search-result");

      // Create a new heading element for the search result title, with a link to the full article
      const title = document.createElement("h3");
      const link = document.createElement("a");
      link.href = result.url;
      link.textContent = result.url;
      title.appendChild(link);

      // Create a new paragraph element for the search result description
      const description = document.createElement("p");
      description.textContent = result.page;

      // Add the title and description to the list item
      listItem.appendChild(title);
      listItem.appendChild(description);

      // Add the list item to the unordered list of search results
      resultList.appendChild(listItem);
    });

    // Add the unordered list of search results to the page
    searchResults.appendChild(resultList);

    // Create and add page navigation buttons
    const nav = document.createElement("nav");
    for (let i = 1; i <= totalPages; i++) {
      const button = document.createElement("button");
      button.textContent = i;
      button.classList.add("page-button");

      if (i === page) {
        button.disabled = true;
      }

      button.addEventListener("click", (event) => search(event, i));
      nav.appendChild(button);
    }
    searchResults.appendChild(nav);
  } catch (error) {
    // If there's an error fetching search results, display an error message on the page
    console.error("Error fetching search results:", error);
    searchResults.innerHTML =
      "<p>There was an error fetching search results. Please try again later.</p>";
  }
}


const dummyDataLong = Array.from({ length: 200 }, (_, i) => {
  const page = Math.floor(i / 10) + 1;
  return { url: `https://example.com/result/${i + 1}`, page: `This is a dummy description for the result item ${page}` };
});



const dummyData1 = [
  { url: "https://example.com/search/1", page: "Here's a short description of the first search result." },
  { url: "https://example.com/search/2", page: "Here's a short description of the second search result." },
  { url: "https://example.com/search/3", page: "Here's a short description of the third search result." },
  { url: "https://example.com/search/4", page: "Here's a short description of the fourth search result." },
  { url: "https://example.com/search/5", page: "Here's a short description of the fifth search result." },
];

const dummyData2 = [
  { url: "https://example.com/articles/1", page: "Learn how to make a delicious and satisfying cup of coffee at home, including tips for choosing the best beans and brewing methods." },
  { url: "https://example.com/articles/2", page: "Discover the many ways that practicing yoga can improve your physical and mental health, including increased flexibility, strength, and relaxation." },
  { url: "https://example.com/articles/3", page: "Get inspired for your next meal with these simple and nutritious dinner ideas, perfect for busy weeknights or lazy weekends." },
  { url: "https://example.com/articles/4", page: "Explore the most popular sights and experiences in the City of Light, from the iconic Eiffel Tower to the charming streets of Montmartre." },
  { url: "https://example.com/articles/5", page: "Learn the essential steps for launching and growing a profitable online business, including strategies for marketing, sales, and customer service." },
];

const dummyData3 = [
  { url: "https://example.com/guides/1", page: "Discover the most anticipated books of the year, from bestselling authors to up-and-coming debut novelists. Get ready to add these titles to your reading list!" },
  { url: "https://example.com/guides/2", page: "Are you a thrill-seeker looking for your next adventure? Check out these top travel destinations, perfect for hiking, biking, kayaking, and more." },
  { url: "https://example.com/guides/3", page: "Simplify your closet and elevate your style with a capsule wardrobe. Learn how to choose versatile pieces that can be mixed and matched for any occasion." },
  { url: "https://example.com/guides/4", page: "Are you struggling to balance your work and personal life? Check out these time management tips and tricks for boosting your productivity and achieving your goals." },
  { url: "https://example.com/guides/5", page: "Discover the science-backed benefits of meditation for reducing stress and anxiety, improving focus and concentration, and promoting overall well-being." },
];

  
//   // Update your form event listener to call the search function with the initial page number
// const form = document.getElementById("search-form");
// form.addEventListener("submit", (event) => search(event, 1));